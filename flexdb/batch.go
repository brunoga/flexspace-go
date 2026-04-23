package flexdb

import (
	"bytes"
	"context"
	"errors"
	"time"
)

// Batch collects Put/Delete operations across one or more tables and commits
// them atomically: written as a single WAL record, then applied to the shared
// memtable under one lock acquisition.
//
// Conditions added via Check are evaluated atomically during Commit under the
// same write-lock acquisition as the writes. If any condition fails, no writes
// are applied and Commit returns (false, nil).
type Batch struct {
	db     *DB
	ops    []batchOp
	checks []batchCheck
}

type batchOp struct {
	table *Table
	key   []byte // user key (without tableID prefix)
	value []byte // nil = delete (tombstone)
}

type batchCheck struct {
	table    *Table
	key      []byte // user key
	expected []byte // nil = key must be absent
}

// NewBatch creates a new empty write batch.
func (db *DB) NewBatch() *Batch {
	return &Batch{db: db}
}

// Put adds a put operation to the batch.
func (b *Batch) Put(table *Table, key, value []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	b.ops = append(b.ops, batchOp{table: table, key: k, value: v})
}

// Delete adds a delete (tombstone) operation to the batch.
func (b *Batch) Delete(table *Table, key []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	b.ops = append(b.ops, batchOp{table: table, key: k, value: nil})
}

// Check adds a pre-condition: the batch will only be committed if the current
// value of key equals expected at the time of Commit. Pass nil expected to
// require the key to be absent.
func (b *Batch) Check(table *Table, key, expected []byte) {
	k := make([]byte, len(key))
	copy(k, key)
	var e []byte
	if expected != nil {
		e = make([]byte, len(expected))
		copy(e, expected)
	}
	b.checks = append(b.checks, batchCheck{table: table, key: k, expected: e})
}

// Commit atomically evaluates all conditions and, if they all pass, writes all
// operations to the WAL and applies them to the shared memtable.
//
// Returns (true, nil) if all conditions passed and the writes were applied.
// Returns (false, nil) if any condition failed; no writes are applied.
// Returns (false, err) on a system error.
func (b *Batch) Commit(ctx context.Context) (bool, error) {
	if len(b.ops) == 0 && len(b.checks) == 0 {
		return true, nil
	}
	db := b.db

	// Validate all ops before touching shared state.
	for _, op := range b.ops {
		if len(op.key) == 0 || len(op.key) > MaxKVSize {
			return false, ErrInvalidKV
		}
		if len(op.value) > MaxBlobSize {
			return false, ErrBlobTooLarge
		}
	}

	// Write blob values to the blob store before acquiring any locks.
	// Large values are replaced with their 16-byte sentinels in b.ops so that
	// the WAL record and skip-list entries only ever hold small sentinels.
	// Blob writes are fdatasynced inside blobs.write, ensuring that if the
	// sentinel reaches the WAL the blob bytes are already on disk.
	for i := range b.ops {
		op := &b.ops[i]
		if len(op.key)+len(op.value) > MaxKVSize && len(op.value) > 0 {
			offset, err := op.table.blobs.write(op.value)
			if err != nil {
				return false, err
			}
			sentinel := make([]byte, blobSentinelSize)
			encodeBlobSentinel(sentinel, offset, uint32(len(op.value)))
			op.value = sentinel
		}
	}

	// Build tableID-prefixed keys for each op.
	pops := make([]walBatchOp, len(b.ops))
	for i, op := range b.ops {
		pops[i] = walBatchOp{
			pkey:  makePrefixedKey(op.table.id, op.key),
			value: op.value,
		}
	}

	// Wait until the active memtable has capacity (same as Put).
	maxWait := time.Now().Add(30 * time.Second)
	for db.activeMT().isFull() {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(time.Millisecond):
		}
		if time.Now().After(maxWait) {
			return false, errors.New("flexdb: timeout waiting for memtable space (is flush worker running?)")
		}
	}

	// Acquire ALL memtable write locks to ensure atomicity.
	for i := range db.rwMT {
		db.rwMT[i].Lock()
	}
	defer func() {
		for i := range db.rwMT {
			db.rwMT[i].Unlock()
		}
	}()

	// Evaluate conditions. Reads under the write locks are safe: no swap can
	// occur while we hold all 16 locks, so the active MT is stable.
	for _, chk := range b.checks {
		cur, err := b.readCurrent(chk.table, chk.key)
		if err != nil {
			return false, err
		}
		if !bytes.Equal(cur, chk.expected) {
			return false, nil
		}
	}

	seq := db.seqNum.Add(1)
	mt := db.activeMT()

	// Write one atomic WAL record for the entire batch.
	if err := mt.logAppendBatch(pops, seq); err != nil {
		return false, err
	}

	// Apply all ops to the skip list.
	for _, op := range pops {
		sz := int64(kv128EncodedSize(len(op.pkey), len(op.value)))
		mt.m.Set(op.pkey, op.value)
		mt.size.Add(sz)
	}

	return true, nil
}

// readCurrent reads the current value of key in table, checking active MT →
// immutable MT → flexfile. Must be called while holding all rwMT write locks.
// Returns (nil, nil) if the key is absent or has a tombstone. Blob sentinels
// are expanded so that callers compare against actual blob bytes.
func (b *Batch) readCurrent(table *Table, key []byte) ([]byte, error) {
	db := b.db
	pkey := makePrefixedKey(table.id, key)

	// Active memtable (hidden cannot be true: we hold all write locks).
	if val, ok := db.activeMT().get(pkey); ok {
		if len(val) == 0 {
			return nil, nil // tombstone
		}
		return table.expandBlob(val)
	}
	// Immutable memtable (also stable: swap requires all write locks).
	if val, ok := db.immutableMT().get(pkey); ok {
		if len(val) == 0 {
			return nil, nil
		}
		return table.expandBlob(val)
	}
	// Flexfile (lock order: rwMT → rwFF, same as the flush path).
	h := hash32(key)
	ffLockID := h & (lockShards - 1)
	table.rwFF[ffLockID].RLock()
	val := table.getPassthrough(key, table.itvbuf, fp16(h))
	table.rwFF[ffLockID].RUnlock()
	if len(val) == 0 {
		return nil, nil
	}
	return table.expandBlob(val)
}
