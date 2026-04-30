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
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DB is the top-level FlexDB database manager. It holds a table registry and
// a shared write-ahead log that all tables write through.
type DB struct {
	path  string
	capMB uint64

	memtableCap int64
	flushInter  time.Duration
	maxWALBytes int64

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

	// Stores the first error encountered in the background flush path.
	// Returned by Sync() and Close(). Protected by flushErrMu.
	flushErrMu sync.Mutex
	flushErr   error

	// 16-shard read-write locks for shared memtable access.
	rwMT [lockShards]sync.RWMutex

	// Monotonically increasing sequence number. Incremented on every committed
	// write (single-op or batch). Stored in WAL records; restored on replay.
	// Enables MVCC and conflict detection without format migration later.
	seqNum atomic.Uint64

	// performance metrics
	metrics Metrics
}

const (
	lockShards = 16

	tableRegistryMagic   = 0x46524547 // 'FREG'
	tableRegistryVersion = 1

	// memtableFlushBatch is the internal batch size used by flushMT.
	memtableFlushBatch = 1024
)

// Options contains configuration for a FlexDB instance.
type Options struct {
	// CacheMB is the per-table in-memory cache capacity in megabytes (default: 64).
	CacheMB uint64

	// MemtableCap is the capacity of each memtable in bytes (default: 1 GiB).
	MemtableCap int64

	// FlushInterval is the interval at which memtables are flushed to disk (default: 5s).
	FlushInterval time.Duration

	// MaxWALBytes is the maximum WAL file size in bytes before a forced memtable flush
	// is triggered (default: 256 MiB). Set to 0 to disable the WAL size limit.
	// This guards against unbounded WAL growth when the flush interval is long or
	// writes are infrequent and never trigger a memtable-full flush.
	MaxWALBytes int64
}

// DefaultOptions returns a set of sensible default options.
func DefaultOptions() *Options {
	return &Options{
		CacheMB:       64,
		MemtableCap:   1024 << 20,
		FlushInterval: 5 * time.Second,
		MaxWALBytes:   256 << 20,
	}
}

// Open opens or creates a FlexDB at the given path.
func Open(ctx context.Context, path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	if err := os.MkdirAll(filepath.Join(path, "tables"), 0700); err != nil {
		return nil, fmt.Errorf("flexdb open: mkdir: %w", err)
	}

	db := &DB{
		path:         path,
		capMB:        opts.CacheMB,
		memtableCap:  opts.MemtableCap,
		flushInter:   opts.FlushInterval,
		maxWALBytes:  opts.MaxWALBytes,
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
	if err := db.loadTablesRegistry(ctx); err != nil {
		_ = db.closeAll()
		return nil, fmt.Errorf("flexdb open: load tables: %w", err)
	}

	// Replay WAL logs into the memtables now that tables are loaded.
	var maxSeq uint64
	for i, mt := range db.memtables {
		logPath := filepath.Join(db.path, fmt.Sprintf("MEMTABLE_LOG%d", i))
		seq, err := mt.redoLog(logPath)
		if err != nil {
			_ = db.closeAll()
			return nil, fmt.Errorf("flexdb open: replay WAL %d: %w", i, err)
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	db.seqNum.Store(maxSeq)

	// Flush replayed entries to the correct tables' flexfiles.
	needsSync := false
	for _, mt := range db.memtables {
		if mt.m.First().Valid() {
			needsSync = true
			if err := db.flushMT(mt); err != nil {
				slog.Error("flexdb open: WAL replay flushMT error", "err", err)
			} else {
				if err := mt.truncateLog(); err != nil {
					slog.Error("flexdb open: WAL replay truncateLog error", "err", err)
				}
				mt.reset()
			}
		}
	}
	if needsSync {
		if err := db.Sync(ctx); err != nil {
			slog.Error("flexdb open: initial Sync failed", "err", err)
		}
	}

	// Truncate log files and write fresh timestamp headers.
	for i, mt := range db.memtables {
		if err := mt.truncateLog(); err != nil {
			_ = db.closeAll()
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
func (db *DB) Table(ctx context.Context, name string) (*Table, error) {
	db.tablesMu.Lock()
	defer db.tablesMu.Unlock()

	if t, ok := db.tablesByName[name]; ok {
		return t, nil
	}

	db.nextID++
	id := db.nextID

	t, err := db.openTable(ctx, id, name)
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
func (db *DB) DropTable(ctx context.Context, name string) error {
	// Flush to ensure no pending writes for this table are left in the memtable.
	// Ignore sync errors here; DropTable is best-effort.
	_ = db.Sync(ctx)

	db.tablesMu.Lock()
	defer db.tablesMu.Unlock()

	t, ok := db.tablesByName[name]
	if !ok {
		return nil // already gone
	}

	_ = t.close()
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

// setFlushErr stores the first flush-path error encountered.
func (db *DB) setFlushErr(err error) {
	if err == nil {
		return
	}
	db.flushErrMu.Lock()
	if db.flushErr == nil {
		db.flushErr = err
	}
	db.flushErrMu.Unlock()
}

// Close flushes all pending data and releases resources.
func (db *DB) Close() error {
	syncErr := db.Sync(context.Background())
	if db.flushWorker != nil {
		db.flushWorker.stop()
	}
	closeErr := db.closeAll()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (db *DB) closeAll() error {
	var firstErr error
	for _, mt := range db.memtables {
		if mt != nil {
			if err := mt.close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	db.tablesMu.RLock()
	defer db.tablesMu.RUnlock()
	for _, t := range db.tablesByID {
		if err := t.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Sync forces a full flush of all pending writes to the flexfiles.
// Returns the first error encountered during this Sync call, if any.
// Each call resets the error state so transient failures are not reported
// indefinitely; the returned error always reflects the current flush.
func (db *DB) Sync(ctx context.Context) error {
	db.flushErrMu.Lock()
	db.flushErr = nil
	db.flushErrMu.Unlock()

	db.flushActiveMT()
	// If the immutable memtable still has pending data from a previously
	// failed flush (e.g., after storage error recovery), retry it now so
	// Sync provides the expected durability guarantee.
	if immut := db.immutableMT(); immut.size.Load() > 0 {
		db.swapAndFlush()
	}
	db.tablesMu.RLock()
	for _, t := range db.tablesByID {
		select {
		case <-ctx.Done():
			db.tablesMu.RUnlock()
			return ctx.Err()
		default:
		}
		if err := t.ff.Sync(); err != nil {
			db.setFlushErr(err)
		}
		if err := t.blobs.sync(); err != nil {
			db.setFlushErr(err)
		}
	}
	db.tablesMu.RUnlock()
	db.flushErrMu.Lock()
	err := db.flushErr
	db.flushErrMu.Unlock()
	return err
}

// DBStats holds database-level statistics.
type DBStats struct {
	// Seq is the sequence number of the last committed write.
	Seq uint64
	// TableCount is the total number of open tables.
	TableCount int
	// ActiveMTBytes is the number of bytes currently in the active memtable.
	ActiveMTBytes int64
	// InactiveMTBytes is the number of bytes in the immutable (flushing) memtable.
	InactiveMTBytes int64
}

// Stats returns a snapshot of database-level statistics.
func (db *DB) Stats() DBStats {
	db.tablesMu.RLock()
	tableCount := len(db.tablesByName)
	db.tablesMu.RUnlock()
	return DBStats{
		Seq:             db.seqNum.Load(),
		TableCount:      tableCount,
		ActiveMTBytes:   int64(db.activeMT().size.Load()),
		InactiveMTBytes: int64(db.immutableMT().size.Load()),
	}
}

// Metrics returns a point-in-time copy of database performance metrics.
func (db *DB) Metrics() MetricsSnapshot {
	return db.metrics.Snapshot()
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
// Errors are stored via setFlushErr and can be retrieved from Sync()/Close().
func (db *DB) swapAndFlush() {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	immut := db.immutableMT()
	if immut.size.Load() > 0 {
		// Previous flush failed. Rebuild each table's in-memory index from disk
		// so the retry sees a state consistent with the on-disk flexfile rather
		// than the partially-updated tree left by the failed attempt.
		db.tablesMu.RLock()
		for _, t := range db.tablesByID {
			for i := range t.rwFF {
				t.rwFF[i].Lock()
			}
			t.tree = newDBTree()
			t.cache = newDBCache(db, t.ff, db.capMB)
			if t.ff.Size() > 0 {
				if err := t.rebuildIndex(); err != nil {
					slog.Error("flexdb: rebuildIndex failed on flush retry", "err", err)
				}
			}
			for i := range t.rwFF {
				t.rwFF[i].Unlock()
			}
		}
		db.tablesMu.RUnlock()

		// Try again before swapping.
		if err := db.flushMT(immut); err != nil {
			db.setFlushErr(err)
			return
		}
		if err := immut.truncateLog(); err != nil {
			db.setFlushErr(fmt.Errorf("flexdb: truncate WAL: %w", err))
		}
		for i := range db.rwMT {
			db.rwMT[i].Lock()
		}
		immut.hidden.Store(true)
		immut.reset()
		immut.hidden.Store(false)
		for i := range db.rwMT {
			db.rwMT[i].Unlock()
		}
	}

	for i := range db.rwMT {
		db.rwMT[i].Lock()
	}
	old := db.activeMTIdx.Load()
	db.activeMTIdx.Store(1 - old)
	for i := range db.rwMT {
		db.rwMT[i].Unlock()
	}

	immut = db.memtables[old]

	if err := immut.flushLog(); err != nil {
		db.setFlushErr(fmt.Errorf("flexdb: flush WAL: %w", err))
	}
	db.metrics.WALFlushCount.Add(1)

	if err := db.flushMT(immut); err != nil {
		db.setFlushErr(err)
		// Don't truncate WAL and don't reset memtable. We keep the data
		// in memory so it can be retried and remains visible to Get().
		return
	}

	db.metrics.FlushBatchCount.Add(1)
	db.tablesMu.RLock()
	for _, t := range db.tablesByID {
		if err := t.ff.Sync(); err != nil {
			db.setFlushErr(err)
		}
		if err := t.blobs.sync(); err != nil {
			db.setFlushErr(err)
		}
	}
	db.tablesMu.RUnlock()

	if err := immut.truncateLog(); err != nil {
		db.setFlushErr(fmt.Errorf("flexdb: truncate WAL: %w", err))
	}

	for i := range db.rwMT {
		db.rwMT[i].Lock()
	}
	immut.hidden.Store(true)
	immut.reset()
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
func (db *DB) flushMT(mt *memtable) error {
	var nh nodeHandler
	var currentTableID uint32
	var currentTable *Table

	type entry struct{ key, value []byte }
	var batch [memtableFlushBatch]entry
	batchSize := 0

	var flushErr error
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
			var err error
			func() {
				t.rwFF[lockID].Lock()
				defer t.rwFF[lockID].Unlock()
				if len(e.value) == 0 {
					err = t.deletePassthrough(userKey, &nh, fp16(h))
				} else {
					err = t.putPassthrough(&KV{Key: userKey, Value: e.value}, &nh)
				}
			}()
			if err != nil && flushErr == nil {
				flushErr = err
			}
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
	return flushErr
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

// loadTablesRegistry reads the TABLE_REGISTRY file and opens each table's flexfile.
func (db *DB) loadTablesRegistry(ctx context.Context) error {
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

	var n int
	off := 0

	// Check for magic number to distinguish between legacy and v1+ formats.
	magic := binary.LittleEndian.Uint32(data[off:])
	if magic == tableRegistryMagic {
		off += 4
		if len(data) < 12 {
			return fmt.Errorf("table registry: truncated header")
		}
		version := binary.LittleEndian.Uint32(data[off:])
		off += 4
		if version > tableRegistryVersion {
			return fmt.Errorf("table registry: unsupported version %d", version)
		}
		n = int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
	} else {
		// Legacy format: first 4 bytes is n_tables.
		n = int(magic)
		off += 4
	}

	for i := 0; i < n && off < len(data); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if off+2 > len(data) {
			break
		}
		nameLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+nameLen+4 > len(data) {
			break
		}
		name := string(data[off : off+nameLen])
		off += nameLen
		id := binary.LittleEndian.Uint32(data[off:])
		off += 4

		t, err := db.openTable(ctx, id, name)
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
	size := 4 + 4 + 4 // magic (4) + version (4) + n_tables (4)
	for name := range db.tablesByName {
		size += 2 + len(name) + 4 // name_len + name + id
	}

	buf := make([]byte, size)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:], tableRegistryMagic)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], tableRegistryVersion)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(db.tablesByName)))
	off += 4
	for name, t := range db.tablesByName {
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(name)))
		off += 2
		copy(buf[off:], name)
		off += len(name)
		binary.LittleEndian.PutUint32(buf[off:], t.id)
		off += 4
	}

	// Atomic write via rename.
	tmp := filepath.Join(db.path, "TABLE_REGISTRY.tmp")
	if err := os.WriteFile(tmp, buf[:off], 0600); err != nil {
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

// StartWorker starts the background flush worker.
// Normally called automatically by Open(), but can be used to restart it if stopped.
func (db *DB) StartWorker() {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()
	if db.flushWorker != nil {
		return
	}
	fw := &flushWorker{
		db:   db,
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}
	db.flushWorker = fw
	fw.start()
}
