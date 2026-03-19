package flexkv

// Batch is an index-aware multi-operation write batch.
//
// All operations — primary key writes and every secondary-index mutation they
// imply — are assembled into a single flexdb.Batch and committed as one WAL
// record under one lock acquisition.  The result is the same guarantee as a
// single Table.Put: either every data write and every index write succeeds
// together, or none do.
//
// Pre-condition checks (Check) are also evaluated atomically with the writes.
// If any check fails, Commit returns (false, nil) and nothing is written.
//
// Usage:
//
//	b := db.NewBatch()
//	b.Check(tblOrders, []byte("order-1"), []byte("pending"))
//	b.Put(tblOrders, []byte("order-1"), []byte("shipped"))
//	b.Delete(tblDrafts, []byte("draft-1"))
//	ok, err := b.Commit()
//
// Duplicate keys: if Put/Delete is called more than once for the same
// (table, key) pair, only the last op for that pair is applied; earlier
// writes to the same key are discarded before index computation begins.
//
// Concurrency: the same narrow race that exists in Table.Put (reading the old
// value for index removal outside the write-lock) applies here.  User-supplied
// Check entries cover the typical CAS use-case.  If concurrent index-writer
// safety is required, add explicit Check entries for every key you modify.
type Batch struct {
	db     *DB
	ops    []batchOp
	checks []batchCheck
}

type batchOp struct {
	table *Table
	key   []byte
	value []byte // nil = delete
}

type batchCheck struct {
	table    *Table
	key      []byte
	expected []byte // nil = key must be absent
}

// NewBatch returns an empty Batch for this database.
func (db *DB) NewBatch() *Batch {
	return &Batch{db: db}
}

// Put queues an insert or update.  A nil value is treated as a delete.
func (b *Batch) Put(table *Table, key, value []byte) {
	b.ops = append(b.ops, batchOp{table: table, key: key, value: value})
}

// Delete queues the removal of key and its index entries.
func (b *Batch) Delete(table *Table, key []byte) {
	b.ops = append(b.ops, batchOp{table: table, key: key})
}

// Check queues a pre-condition.  expected=nil asserts the key must be absent;
// otherwise the stored value must equal expected at the moment of Commit.
func (b *Batch) Check(table *Table, key, expected []byte) {
	b.checks = append(b.checks, batchCheck{table: table, key: key, expected: expected})
}

// Commit assembles and atomically commits the batch.
//
// For each op it reads the current value (to identify stale index entries),
// then builds one flexdb.Batch that contains all user checks, all index
// mutations, and all data writes.  That batch is committed as a single WAL
// record.
//
// Returns (true, nil) on success.
// Returns (false, nil) if any user-supplied check fails; no writes are applied.
// Returns (false, err) on a system error.
func (b *Batch) Commit() (bool, error) {
	// Deduplicate: for each (table, key) pair keep only the last op.
	ops := deduplicateBatchOps(b.ops)

	// Read current values outside the write lock (same as Table.Put).
	// We need them to compute which old index entries to remove.
	type opState struct {
		batchOp
		oldValue []byte
	}
	states := make([]opState, len(ops))
	for i, op := range ops {
		old, err := op.table.dataFDB.NewRef().Get(op.key)
		if err != nil {
			return false, err
		}
		states[i] = opState{batchOp: op, oldValue: old}
	}

	// Build the underlying flexdb.Batch.
	fb := b.db.fdb.NewBatch()

	// User-supplied pre-conditions.
	for _, chk := range b.checks {
		fb.Check(chk.table.dataFDB, chk.key, chk.expected)
	}

	// Per-op: index mutations + data write.
	for _, s := range states {
		// Snapshot the index set while holding the read lock (same pattern as
		// Table.Put, which guards against concurrent CreateIndex / DropIndex).
		s.table.mu.RLock()
		idxSnapshot := make(map[string]indexEntry, len(s.table.indexers))
		for name, fn := range s.table.indexers {
			idxSnapshot[name] = indexEntry{
				ft:      s.table.indexFDB[name],
				indexer: fn,
			}
		}
		s.table.mu.RUnlock()

		// Delete stale index entries (based on old value).
		if s.oldValue != nil {
			for _, ie := range idxSnapshot {
				for _, v := range ie.indexer(s.key, s.oldValue) {
					fb.Delete(ie.ft, encodeIndexKey(v, s.key))
				}
			}
		}

		if s.value != nil {
			// Insert new index entries and the data record.
			for _, ie := range idxSnapshot {
				for _, v := range ie.indexer(s.key, s.value) {
					fb.Put(ie.ft, encodeIndexKey(v, s.key), []byte{1})
				}
			}
			fb.Put(s.table.dataFDB, s.key, s.value)
		} else {
			fb.Delete(s.table.dataFDB, s.key)
		}
	}

	return fb.Commit()
}

// deduplicateBatchOps returns a slice containing only the last op for each
// (table, key) pair, preserving the relative order of the surviving ops.
func deduplicateBatchOps(ops []batchOp) []batchOp {
	type opKey struct {
		table *Table
		key   string
	}
	// Walk forward to record the index of the last occurrence of each key.
	last := make(map[opKey]int, len(ops))
	for i, op := range ops {
		last[opKey{op.table, string(op.key)}] = i
	}
	out := make([]batchOp, 0, len(last))
	for i, op := range ops {
		if last[opKey{op.table, string(op.key)}] == i {
			out = append(out, op)
		}
	}
	return out
}
