// Package flexkv provides a multi-table key-value store with secondary indexing
// built on top of flexdb.
//
// Each user-visible table T is backed by two kinds of flexdb tables:
//   - "T"            — the data table, stores primaryKey → value
//   - "T.$indexName" — one per index, stores encodedValue+primaryKey → ∅
//
// Index entries are maintained atomically via flexdb write batches: a Put or
// Delete that changes an indexed record writes all data and index mutations as
// a single WAL record, so the database is always consistent across crashes.
//
// Index definitions (name → indexer function) are registered in memory via
// CreateIndex. The index data itself persists on disk; on restart, call
// CreateIndex (or a variant that skips the initial scan) to re-register the
// indexer function so that future writes maintain the index.
//
// Thread safety: DB and Table methods are safe for concurrent use.
package flexkv

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/brunoga/flexspace-go/flexdb"
)

// Indexer extracts indexed values from a primary key-value pair.
// It may return multiple values (multi-valued index) or nil if the record
// should not appear in the index.
type Indexer func(key, value []byte) [][]byte

// DB is a flexkv database backed by a single flexdb.DB.
type DB struct {
	fdb *flexdb.DB
}

// Options controls database behaviour.
type Options struct {
	CacheMB uint64 // per-table cache budget in MB (default: 64)
}

// DefaultOptions returns a set of sensible default options.
func DefaultOptions() *Options {
	return &Options{
		CacheMB: 64,
	}
}

// Open opens or creates a flexkv database at path.
func Open(ctx context.Context, path string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	fdbOpts := flexdb.DefaultOptions()
	fdbOpts.CacheMB = opts.CacheMB
	fdb, err := flexdb.Open(ctx, path, fdbOpts)
	if err != nil {
		return nil, err
	}
	return &DB{fdb: fdb}, nil
}

// Close closes the database.
func (db *DB) Close() error {
	return db.fdb.Close()
}

// RawDB returns the underlying flexdb.DB. Intended for diagnostics only.
func (db *DB) RawDB() *flexdb.DB {
	return db.fdb
}

// Table opens or creates a named table. Multiple calls with the same name
// return equivalent handles.
func (db *DB) Table(ctx context.Context, name string) (*Table, error) {
	dt, err := db.fdb.Table(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("flexkv: table %q data: %w", name, err)
	}
	return &Table{
		db:       db,
		name:     name,
		dataFDB:  dt,
		indexFDB: make(map[string]*flexdb.Table),
		indexers: make(map[string]Indexer),
	}, nil
}

// DropTable removes the data table and all index tables for name (O(1) in data size).
func (db *DB) DropTable(ctx context.Context, name string) error {
	// Drop data table.
	if err := db.fdb.DropTable(ctx, name); err != nil {
		return fmt.Errorf("flexkv: drop table %q: %w", name, err)
	}
	// Drop all index tables with the prefix "name.$".
	all, err := db.fdb.Tables()
	if err != nil {
		return err
	}
	prefix := name + ".$"
	for _, t := range all {
		if len(t) > len(prefix) && t[:len(prefix)] == prefix {
			if err := db.fdb.DropTable(ctx, t); err != nil {
				return fmt.Errorf("flexkv: drop index table %q: %w", t, err)
			}
		}
	}
	return nil
}

// Tables returns the names of all user-visible tables (excluding internal index tables).
func (db *DB) Tables() ([]string, error) {
	all, err := db.fdb.Tables()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, name := range all {
		// Index tables are named "tableName.$indexName"; skip them.
		if !containsDot(name) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}

func containsDot(s string) bool {
	for _, c := range s {
		if c == '.' {
			return true
		}
	}
	return false
}

// ---- Table ----

// Table represents a user-visible named table.
type Table struct {
	db   *DB
	name string

	mu       sync.RWMutex
	dataFDB  *flexdb.Table            // primary data
	indexFDB map[string]*flexdb.Table // index name → flexdb table
	indexers map[string]Indexer       // index name → indexer function (in-memory)
}

// indexFDBTable returns or opens the flexdb table for the given index.
// Caller must hold t.mu in write mode.
func (t *Table) indexFDBTable(ctx context.Context, indexName string) (*flexdb.Table, error) {
	if ft, ok := t.indexFDB[indexName]; ok {
		return ft, nil
	}
	fdbName := t.name + ".$" + indexName
	ft, err := t.db.fdb.Table(ctx, fdbName)
	if err != nil {
		return nil, err
	}
	t.indexFDB[indexName] = ft
	return ft, nil
}

// indexEntry pairs an index's flexdb table with its indexer function.
type indexEntry struct {
	ft      *flexdb.Table
	indexer Indexer
}

// Put inserts or updates a key-value pair, maintaining all registered indexes atomically.
// Pass nil value to delete (equivalent to Delete).
func (t *Table) Put(ctx context.Context, key, value []byte) error {
	// Snapshot all index state while holding the read lock.
	t.mu.RLock()
	indexes := make(map[string]indexEntry, len(t.indexers))
	for name, indexer := range t.indexers {
		indexes[name] = indexEntry{ft: t.indexFDB[name], indexer: indexer}
	}
	t.mu.RUnlock()

	// Fetch old value to remove stale index entries (no lock needed).
	var oldValue []byte
	if len(indexes) > 0 {
		var err error
		oldValue, err = t.dataFDB.NewRef().Get(ctx, key)
		if err != nil {
			return err
		}
	}

	// Build the write batch.
	batch := t.db.fdb.NewBatch()

	// Remove stale index entries.
	if oldValue != nil {
		for _, ie := range indexes {
			for _, v := range ie.indexer(key, oldValue) {
				batch.Delete(ie.ft, encodeIndexKey(v, key))
			}
		}
	}

	// Add new index entries and the data record.
	if value != nil {
		for _, ie := range indexes {
			for _, v := range ie.indexer(key, value) {
				batch.Put(ie.ft, encodeIndexKey(v, key), []byte{1})
			}
		}
		batch.Put(t.dataFDB, key, value)
	} else {
		batch.Delete(t.dataFDB, key)
	}

	_, err := batch.Commit(ctx)
	return err
}

// Get retrieves a value by primary key. Returns (nil, nil) if not found.
func (t *Table) Get(ctx context.Context, key []byte) ([]byte, error) {
	return t.dataFDB.NewRef().Get(ctx, key)
}

// Delete removes a key-value pair, cleaning up index entries.
func (t *Table) Delete(ctx context.Context, key []byte) error {
	return t.Put(ctx, key, nil)
}

// RegisterIndex re-attaches an indexer function to an index whose data is
// already on disk. Unlike CreateIndex it does not scan existing records —
// use this on restart to restore indexers without redundant work.
func (t *Table) RegisterIndex(ctx context.Context, name string, indexer Indexer) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.indexers[name] = indexer
	_, err := t.indexFDBTable(ctx, name)
	return err
}

// CreateIndex registers indexer under name and populates it from existing records.
// Safe to call concurrently with other operations.
func (t *Table) CreateIndex(ctx context.Context, name string, indexer Indexer) error {
	t.mu.Lock()
	t.indexers[name] = indexer
	ft, err := t.indexFDBTable(ctx, name)
	t.mu.Unlock()
	if err != nil {
		return err
	}

	// Populate from existing data.
	it := t.Scan(nil, nil)
	defer it.Close()

	for ; it.Valid(); it.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		key := it.Key()
		value := it.Value()
		for _, v := range indexer(key, value) {
			batch := t.db.fdb.NewBatch()
			batch.Put(ft, encodeIndexKey(v, key), []byte{1})
			if _, err := batch.Commit(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// RawDataTable returns the flexdb table backing this table's primary data.
// Intended for diagnostics only.
func (t *Table) RawDataTable() *flexdb.Table {
	return t.dataFDB
}

// RawIndexTable returns the flexdb table for the named index, if it has been opened.
// Intended for diagnostics only.
func (t *Table) RawIndexTable(name string) (*flexdb.Table, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ft, ok := t.indexFDB[name]
	return ft, ok
}

// DropIndex removes an index (O(1) via flexdb.DropTable).
func (t *Table) DropIndex(ctx context.Context, name string) error {
	t.mu.Lock()
	delete(t.indexers, name)
	delete(t.indexFDB, name)
	t.mu.Unlock()

	fdbName := t.name + ".$" + name
	return t.db.fdb.DropTable(ctx, fdbName)
}

// Scan returns an iterator over [start, end) on primary keys.
// A nil start seeks to the beginning; a nil end has no upper bound.
func (t *Table) Scan(start, end []byte) *Iterator {
	it := t.dataFDB.NewRef().NewIterator()
	it.Seek(start)
	return &Iterator{it: it, end: end}
}

// ScanPrefix returns an iterator over all entries whose key starts with prefix.
func (t *Table) ScanPrefix(prefix []byte) *Iterator {
	it := t.dataFDB.NewRef().NewIterator()
	it.Seek(prefix)
	// End key = prefix with last byte incremented (or nil if overflow).
	var end []byte
	if len(prefix) > 0 {
		end = make([]byte, len(prefix))
		copy(end, prefix)
		for i := len(end) - 1; i >= 0; i-- {
			end[i]++
			if end[i] != 0 {
				break
			}
			if i == 0 {
				end = nil // overflow: no upper bound
			}
		}
	}
	return &Iterator{it: it, prefix: prefix, end: end}
}

// Index returns a handle to a named secondary index.
func (t *Table) Index(name string) *Index {
	return &Index{t: t, name: name}
}

// ---- Iterator ----

// Iterator iterates over a table's primary key space.
type Iterator struct {
	it     *flexdb.Iterator
	prefix []byte // optional prefix filter (for ScanPrefix)
	end    []byte // exclusive upper bound on user key (nil = unbounded)
}

func (it *Iterator) Valid() bool {
	if !it.it.Valid() {
		return false
	}
	kv := it.it.Current()
	if it.prefix != nil && !bytes.HasPrefix(kv.Key, it.prefix) {
		return false
	}
	if it.end != nil && bytes.Compare(kv.Key, it.end) >= 0 {
		return false
	}
	return true
}

func (it *Iterator) Next()         { it.it.Next() }
func (it *Iterator) Close()        { it.it.Close() }
func (it *Iterator) Key() []byte   { return it.it.Current().Key }
func (it *Iterator) Value() []byte { return it.it.Current().Value }

// ---- Index ----

// Index represents a secondary index on a Table.
type Index struct {
	t    *Table
	name string
}

// fdbTable returns the flexdb table for this index (opens it if needed).
func (idx *Index) fdbTable() (*flexdb.Table, error) {
	idx.t.mu.Lock()
	defer idx.t.mu.Unlock()
	return idx.t.indexFDBTable(context.Background(), idx.name)
}

// Scan returns an iterator over index entries whose indexed value is in [start, end).
// A nil start seeks to the first entry; a nil end has no upper bound.
func (idx *Index) Scan(start, end []byte) *IndexIterator {
	ft, err := idx.fdbTable()
	if err != nil {
		return &IndexIterator{err: err}
	}

	var startKey []byte
	if start != nil {
		startKey = encodeIndexKey(start, nil)
	}
	var endKey []byte
	if end != nil {
		endKey = encodeIndexKey(end, nil)
	}

	it := ft.NewRef().NewIterator()
	it.Seek(startKey)

	return &IndexIterator{it: it, t: idx.t, endKey: endKey}
}

// ScanPrefix returns an iterator over all index entries whose indexed value
// starts with prefix.
func (idx *Index) ScanPrefix(prefix []byte) *IndexIterator {
	ft, err := idx.fdbTable()
	if err != nil {
		return &IndexIterator{err: err}
	}

	// Encode the prefix without the \x00\x00 terminator so we seek to the
	// first index key whose value field starts with prefix.
	enc := encodeIndexValue(prefix)
	startKey := enc[:len(enc)-2] // strip terminator

	// End key = startKey with last byte incremented (same overflow logic as
	// Table.ScanPrefix).
	var endKey []byte
	if len(startKey) > 0 {
		endKey = make([]byte, len(startKey))
		copy(endKey, startKey)
		for i := len(endKey) - 1; i >= 0; i-- {
			endKey[i]++
			if endKey[i] != 0 {
				break
			}
			if i == 0 {
				endKey = nil
			}
		}
	}

	it := ft.NewRef().NewIterator()
	it.Seek(startKey)
	return &IndexIterator{it: it, t: idx.t, endKey: endKey}
}

// Get returns an iterator over all records with exactly this indexed value.
func (idx *Index) Get(value []byte) *IndexIterator {
	end := make([]byte, len(value))
	copy(end, value)
	// Increment last byte to get exclusive upper bound for this exact value.
	// encodeIndexKey appends the null terminator, so scanning [encode(v), encode(v+1))
	// covers all primary keys for value v.
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			break
		}
		if i == 0 {
			end = nil
		}
	}
	return idx.Scan(value, end)
}

// ---- IndexIterator ----

// IndexIterator iterates over secondary index entries.
type IndexIterator struct {
	it     *flexdb.Iterator
	t      *Table
	endKey []byte // exclusive upper bound on encoded index key (nil = unbounded)
	err    error  // sticky error (e.g. from opening the index table)
}

func (it *IndexIterator) Valid() bool {
	if it.err != nil || it.it == nil || !it.it.Valid() {
		return false
	}
	k := it.it.Current().Key
	if it.endKey != nil && bytes.Compare(k, it.endKey) >= 0 {
		return false
	}
	return true
}

func (it *IndexIterator) Next() {
	if it.it != nil {
		it.it.Next()
	}
}

func (it *IndexIterator) Close() {
	if it.it != nil {
		it.it.Close()
	}
}

// Value returns the indexed value for the current entry.
func (it *IndexIterator) Value() []byte {
	k := it.it.Current().Key
	val, _ := decodeIndexValue(k)
	return val
}

// PrimaryKey returns the primary key for the current index entry.
func (it *IndexIterator) PrimaryKey() []byte {
	k := it.it.Current().Key
	_, consumed := decodeIndexValue(k)
	if consumed == 0 {
		return nil
	}
	return k[consumed:]
}

// GetRecord fetches the full value for the current index entry by primary key.
func (it *IndexIterator) GetRecord(ctx context.Context) ([]byte, error) {
	pk := it.PrimaryKey()
	if pk == nil {
		return nil, ErrInvalidIndexEntry
	}
	return it.t.Get(ctx, pk)
}

// ---- Index key encoding ----
//
// Index key format: nullEscape(indexedValue) + primaryKey
//
// The indexed value is null-terminated-escaped so that:
//   - values of any content and length sort correctly (lexicographic = value order)
//   - the primary key appended after the terminator doesn't interfere with comparison

// encodeIndexKey returns nullEscape(value) + primaryKey.
// If primaryKey is nil, only the escaped value prefix is returned (for range bounds).
func encodeIndexKey(value, primaryKey []byte) []byte {
	enc := encodeIndexValue(value)
	out := make([]byte, len(enc)+len(primaryKey))
	copy(out, enc)
	copy(out[len(enc):], primaryKey)
	return out
}

// encodeIndexValue encodes val for use in an index key so that results sort
// lexicographically in the same order as the original values.
//
// Null bytes are escaped as \x00\xFF; the two-byte sequence \x00\x00 marks
// the end of the encoded value.
func encodeIndexValue(val []byte) []byte {
	nulls := 0
	for _, b := range val {
		if b == 0x00 {
			nulls++
		}
	}
	out := make([]byte, len(val)+nulls+2)
	i := 0
	for _, b := range val {
		if b == 0x00 {
			out[i] = 0x00
			out[i+1] = 0xFF
			i += 2
		} else {
			out[i] = b
			i++
		}
	}
	out[i] = 0x00
	out[i+1] = 0x00
	return out[:i+2]
}

// decodeIndexValue decodes a null-terminated escape-encoded value from the
// front of key, returning the decoded value and the number of bytes consumed.
// Returns (nil, 0) on malformed input.
func decodeIndexValue(key []byte) (value []byte, consumed int) {
	for i := 0; i < len(key); i++ {
		if key[i] != 0x00 {
			continue
		}
		if i+1 >= len(key) {
			return nil, 0
		}
		switch key[i+1] {
		case 0x00: // terminator
			return key[:i], i + 2
		case 0xFF: // escaped null — fall through to slow path
			var out []byte
			out = append(out, key[:i]...)
			i += 2
			for i < len(key) {
				if key[i] == 0x00 {
					if i+1 >= len(key) {
						return nil, 0
					}
					switch key[i+1] {
					case 0x00:
						return out, i + 2
					case 0xFF:
						out = append(out, 0x00)
						i += 2
					default:
						return nil, 0
					}
				} else {
					out = append(out, key[i])
					i++
				}
			}
			return nil, 0
		default:
			return nil, 0
		}
	}
	return nil, 0
}
