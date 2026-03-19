// Package flexdb implements a write-optimised key-value store layered on
// flexfile. Tables are the primary unit of organisation: each Table has its
// own flexfile for O(1) drops while sharing a single write-ahead log
// (memtable pair), flush worker, and cache budget. Internally, memtable keys
// are prefixed with a compact uint32 table ID; the flush worker strips the
// prefix before writing entries to the correct table's flexfile.
//
// Use DB.Table to open or create a table, Table.NewRef to obtain a per-goroutine
// handle, and DB.NewBatch for atomic multi-table writes.
package flexdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// DB is the top-level FlexDB database manager. It holds a table registry and
// a shared write-ahead log that all tables write through.
type DB struct {
	path  string
	capMB uint64

	// Table registry (protected by tablesMu).
	tablesMu     sync.RWMutex
	tablesByName map[string]*Table
	tablesByID   map[uint32]*Table
	nextID       uint32

	// Shared double-buffered memtable pair.
	memtables   [2]*memtable
	activeMTIdx atomic.Uint32
	flushWorker *flushWorker
	flushMu     sync.Mutex // serialises concurrent swapAndFlush calls

	// 16-shard read-write locks for shared memtable access.
	rwMT [lockShards]sync.RWMutex

	// Monotonically increasing sequence number. Incremented on every committed
	// write (single-op or batch). Stored in WAL records; restored on replay.
	// Enables MVCC and conflict detection without format migration later.
	seqNum atomic.Uint64
}

const lockShards = 16

// Open opens or creates a FlexDB at the given path.
// capMB is the per-table in-memory cache capacity in megabytes.
func Open(path string, capMB uint64) (*DB, error) {
	if err := os.MkdirAll(filepath.Join(path, "tables"), 0755); err != nil {
		return nil, fmt.Errorf("flexdb open: mkdir: %w", err)
	}

	db := &DB{
		path:         path,
		capMB:        capMB,
		tablesByName: make(map[string]*Table),
		tablesByID:   make(map[uint32]*Table),
	}

	// Open the two shared memtable WAL log files.
	for i := range db.memtables {
		logPath := filepath.Join(path, fmt.Sprintf("MEMTABLE_LOG%d", i))
		mt, err := newMemtable(db, logPath)
		if err != nil {
			return nil, fmt.Errorf("flexdb open: memtable %d: %w", i, err)
		}
		db.memtables[i] = mt
	}

	// Load existing tables from the registry and open their flexfiles.
	if err := db.loadTablesRegistry(); err != nil {
		db.closeAll()
		return nil, fmt.Errorf("flexdb open: load tables: %w", err)
	}

	// Replay WAL logs into the memtables; restore the sequence counter.
	var maxSeq uint64
	for i, mt := range db.memtables {
		logPath := filepath.Join(db.path, fmt.Sprintf("MEMTABLE_LOG%d", i))
		seq, err := mt.redoLog(logPath)
		if err != nil {
			db.closeAll()
			return nil, fmt.Errorf("flexdb open: replay WAL %d: %w", i, err)
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	db.seqNum.Store(maxSeq)

	// Flush replayed entries to the correct tables' flexfiles.
	for _, mt := range db.memtables {
		if mt.m.First().Valid() {
			db.flushMT(mt)
			mt.clear()
		}
	}

	// Truncate log files and write fresh timestamp headers.
	for i, mt := range db.memtables {
		if err := mt.truncateLog(); err != nil {
			db.closeAll()
			return nil, fmt.Errorf("flexdb open: truncate log %d: %w", i, err)
		}
	}

	// Start background flush worker.
	fw := &flushWorker{
		db:   db,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	db.flushWorker = fw
	fw.start()

	return db, nil
}

// Table opens or creates a named table.
// Multiple calls with the same name return the same handle.
func (db *DB) Table(name string) (*Table, error) {
	db.tablesMu.Lock()
	defer db.tablesMu.Unlock()

	if t, ok := db.tablesByName[name]; ok {
		return t, nil
	}

	db.nextID++
	id := db.nextID

	t, err := db.openTable(id, name)
	if err != nil {
		db.nextID--
		return nil, err
	}

	db.tablesByName[name] = t
	db.tablesByID[id] = t

	if err := db.saveTablesRegistry(); err != nil {
		// Non-fatal; data is safe but registry might be stale on next open.
		_ = err
	}
	return t, nil
}

// DropTable removes a table and all its data (O(1) in data size).
// After this call, any existing TableRef for the table must not be used.
func (db *DB) DropTable(name string) error {
	// Flush to ensure no pending writes for this table are left in the memtable.
	db.Sync()

	db.tablesMu.Lock()
	defer db.tablesMu.Unlock()

	t, ok := db.tablesByName[name]
	if !ok {
		return nil // already gone
	}

	t.ff.Close()
	delete(db.tablesByName, name)
	delete(db.tablesByID, t.id)

	if err := db.saveTablesRegistry(); err != nil {
		_ = err
	}

	return os.RemoveAll(tableDir(db.path, t.id))
}

// Tables returns the names of all open tables in sorted order.
func (db *DB) Tables() ([]string, error) {
	db.tablesMu.RLock()
	defer db.tablesMu.RUnlock()
	names := make([]string, 0, len(db.tablesByName))
	for name := range db.tablesByName {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Close flushes all pending data and releases resources.
func (db *DB) Close() error {
	db.Sync()
	if db.flushWorker != nil {
		db.flushWorker.stop()
	}
	return db.closeAll()
}

func (db *DB) closeAll() error {
	for _, mt := range db.memtables {
		if mt != nil {
			mt.close()
		}
	}
	db.tablesMu.RLock()
	defer db.tablesMu.RUnlock()
	for _, t := range db.tablesByID {
		t.ff.Close()
	}
	return nil
}

// Sync forces a full flush of all pending writes to the flexfiles.
func (db *DB) Sync() {
	if db.flushWorker != nil {
		db.flushWorker.triggerImmediate()
	}
	db.flushActiveMT()
	db.flushActiveMT()
	db.tablesMu.RLock()
	for _, t := range db.tablesByID {
		t.ff.Sync()
	}
	db.tablesMu.RUnlock()
}

// DBStats holds database-level statistics.
type DBStats struct {
	Seq             uint64 // sequence number of the last committed write
	TableCount      int    // number of open tables (includes internal/index tables)
	ActiveMTBytes   int64  // bytes in the active (writable) memtable
	InactiveMTBytes int64  // bytes in the immutable (may be flushing) memtable
}

// Stats returns a snapshot of database-level statistics.
func (db *DB) Stats() DBStats {
	db.tablesMu.RLock()
	tableCount := len(db.tablesByName)
	db.tablesMu.RUnlock()
	return DBStats{
		Seq:             db.seqNum.Load(),
		TableCount:      tableCount,
		ActiveMTBytes:   db.activeMT().size.Load(),
		InactiveMTBytes: db.immutableMT().size.Load(),
	}
}

// Seq returns the sequence number of the last committed write.
// Each Put, Delete, or Batch.Commit atomically increments this counter.
// Sequence numbers are stored in the WAL and restored on reopen.
func (db *DB) Seq() uint64 {
	return db.seqNum.Load()
}

// activeMT returns the currently active (writable) memtable.
func (db *DB) activeMT() *memtable {
	return db.memtables[db.activeMTIdx.Load()]
}

// immutableMT returns the currently immutable (read-only, may be flushing) memtable.
func (db *DB) immutableMT() *memtable {
	return db.memtables[1-db.activeMTIdx.Load()]
}

// swapAndFlush atomically swaps active/immutable memtables, then flushes
// the old active (now immutable) to the appropriate flexfiles.
func (db *DB) swapAndFlush() {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	for i := range db.rwMT {
		db.rwMT[i].Lock()
	}
	old := db.activeMTIdx.Load()
	db.activeMTIdx.Store(1 - old)
	for i := range db.rwMT {
		db.rwMT[i].Unlock()
	}

	immut := db.memtables[old]
	immut.flushLog()
	db.flushMT(immut)

	db.tablesMu.RLock()
	for _, t := range db.tablesByID {
		t.ff.Sync()
	}
	db.tablesMu.RUnlock()

	immut.truncateLog()

	for i := range db.rwMT {
		db.rwMT[i].Lock()
	}
	immut.hidden.Store(true)
	immut.clear()
	immut.hidden.Store(false)
	for i := range db.rwMT {
		db.rwMT[i].Unlock()
	}
}

func (db *DB) flushActiveMT() {
	active := db.activeMT()
	if active.size.Load() == 0 {
		return
	}
	db.swapAndFlush()
}

// flushMT writes all entries in mt to the appropriate table flexfiles.
// Keys in mt are tableID-prefixed; the prefix is stripped before writing.
func (db *DB) flushMT(mt *memtable) {
	var nh nodeHandler
	var currentTableID uint32
	var currentTable *Table

	type entry struct{ key, value []byte }
	var batch [memtableFlushBatch]entry
	batchSize := 0

	flush := func() {
		for i := 0; i < batchSize; i++ {
			e := batch[i]
			if len(e.key) < 4 {
				continue // invalid prefixed key
			}
			tableID := binary.BigEndian.Uint32(e.key[:4])
			userKey := e.key[4:]

			// Resolve table (reset hint when table changes).
			if tableID != currentTableID || currentTable == nil {
				db.tablesMu.RLock()
				t, ok := db.tablesByID[tableID]
				db.tablesMu.RUnlock()
				if !ok {
					continue // table was dropped
				}
				if tableID != currentTableID {
					nh = nodeHandler{} // reset tree-walk hint
				}
				currentTableID = tableID
				currentTable = t
			}

			t := currentTable
			h := hash32(userKey)
			lockID := h & (lockShards - 1)
			t.rwFF[lockID].Lock()
			if len(e.value) == 0 {
				t.deletePassthrough(userKey, &nh, fp16(h))
			} else {
				t.putPassthrough(&KV{Key: userKey, Value: e.value}, &nh)
			}
			t.rwFF[lockID].Unlock()
		}
		batchSize = 0
	}

	for it := mt.m.First(); it.Valid(); it.Next() {
		batch[batchSize] = entry{key: it.Key(), value: it.Value()}
		batchSize++
		if batchSize == memtableFlushBatch {
			flush()
		}
	}
	if batchSize > 0 {
		flush()
	}
}

// ---- memtable locking helpers ----

func (db *DB) enterMT(hash uint32) uint32 {
	id := hash & (lockShards - 1)
	db.rwMT[id].RLock()
	return id
}

func (db *DB) exitMT(id uint32) {
	db.rwMT[id].RUnlock()
}

// ---- file tag helpers ----

const (
	fileTagAnchorBit     = uint16(1)
	fileTagUnsortedShift = 1
)

func fileTagGenerate(isAnchor bool, unsorted uint8) uint16 {
	var tag uint16
	if isAnchor {
		tag |= fileTagAnchorBit
	}
	tag |= uint16(unsorted) << fileTagUnsortedShift
	return tag
}

func fileTagIsAnchor(tag uint16) bool {
	return (tag & fileTagAnchorBit) != 0
}

func fileTagUnsorted(tag uint16) uint8 {
	return uint8(tag >> fileTagUnsortedShift)
}

// ---- table registry persistence ----

// loadTablesRegistry reads the TABLES file and opens each table's flexfile.
func (db *DB) loadTablesRegistry() error {
	path := filepath.Join(db.path, "TABLE_REGISTRY")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) < 4 {
		return nil
	}
	n := int(binary.BigEndian.Uint32(data))
	off := 4
	for i := 0; i < n && off < len(data); i++ {
		if off+2 > len(data) {
			break
		}
		nameLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		if off+nameLen+4 > len(data) {
			break
		}
		name := string(data[off : off+nameLen])
		off += nameLen
		id := binary.BigEndian.Uint32(data[off:])
		off += 4

		t, err := db.openTable(id, name)
		if err != nil {
			return fmt.Errorf("load table %q (id=%d): %w", name, id, err)
		}
		db.tablesByName[name] = t
		db.tablesByID[id] = t
		if id > db.nextID {
			db.nextID = id
		}
	}
	return nil
}

// saveTablesRegistry writes the current table registry atomically to disk.
func (db *DB) saveTablesRegistry() error {
	// Compute required size.
	size := 4 // n_tables (uint32)
	for name := range db.tablesByName {
		size += 2 + len(name) + 4 // name_len + name + id
	}

	buf := make([]byte, size)
	off := 0
	binary.BigEndian.PutUint32(buf[off:], uint32(len(db.tablesByName)))
	off += 4
	for name, t := range db.tablesByName {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(name)))
		off += 2
		copy(buf[off:], name)
		off += len(name)
		binary.BigEndian.PutUint32(buf[off:], t.id)
		off += 4
	}

	// Atomic write via rename.
	tmp := filepath.Join(db.path, "TABLE_REGISTRY.tmp")
	if err := os.WriteFile(tmp, buf[:off], 0644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(db.path, "TABLE_REGISTRY"))
}

// ---- recovery anchor type (shared with table.go rebuildIndex) ----

type recoveryAnchor struct {
	key      []byte
	loff     uint64
	unsorted uint8
}
