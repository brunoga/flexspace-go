package flexdb

import (
	"bytes"
	"errors"
)

// Put inserts or updates a key-value pair in the table. Values larger than
// MaxKVSize−len(key) bytes are stored in the per-table blob file; a 16-byte
// sentinel is written to the main KV store in their place.
// Caller must not hold any DB locks.
func (ref *TableRef) Put(key, value []byte) error {
	t := ref.table
	db := t.db
	if len(key) == 0 || len(key) > MaxKVSize {
		return errInvalidKV
	}
	if len(value) > MaxBlobSize {
		return errBlobTooLarge
	}

	storeValue := value
	if len(key)+len(value) > MaxKVSize && len(value) > 0 {
		// Large value: write to the blob store first (with fdatasync), then
		// store a sentinel in the memtable. The fdatasync ensures that if the
		// sentinel reaches durable storage the blob bytes are already on disk.
		offset, err := t.blobs.write(value)
		if err != nil {
			return err
		}
		sentinel := make([]byte, blobSentinelSize)
		encodeBlobSentinel(sentinel, offset, uint32(len(value)))
		storeValue = sentinel
	}

	pkey := makePrefixedKey(t.id, key)
	h := hash32(pkey)

	for db.activeMT().isFull() {
		// Brief yield; background worker swaps when ready.
	}

	seq := db.seqNum.Add(1)
	lockID := db.enterMT(h)
	err := db.activeMT().put(pkey, storeValue, seq)
	db.exitMT(lockID)
	return err
}

// Get returns a copy of the value for key, or nil if not found.
// For blob values the bytes are read from the blob file into a fresh slice.
func (ref *TableRef) Get(key []byte) ([]byte, error) {
	val, err := ref.GetView(key)
	if err != nil || val == nil {
		return nil, err
	}
	// GetView returns a view into the cache/memtable for inline values; copy
	// it so the caller's slice is independent of internal mutable state.
	// For blob values GetView already returns a fresh allocation, but we copy
	// here too to maintain a uniform contract: Get always owns its result.
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// GetView returns the value for key. For inline values the returned slice is
// only valid until the next mutating operation on the DB. For blob values a
// fresh allocation is always returned (identical behaviour to Get).
func (ref *TableRef) GetView(key []byte) ([]byte, error) {
	t := ref.table
	db := t.db
	pkey := makePrefixedKey(t.id, key)
	h := hash32(pkey)

	// 1. Check active memtable.
	lockID := db.enterMT(h)
	active := db.activeMT()
	if !active.hidden.Load() {
		val, ok := active.get(pkey)
		db.exitMT(lockID)
		if ok {
			if len(val) == 0 {
				return nil, nil // tombstone
			}
			return t.expandBlob(val)
		}
	} else {
		db.exitMT(lockID)
	}

	// 2. Check immutable memtable.
	immut := db.immutableMT()
	if !immut.hidden.Load() {
		val, ok := immut.get(pkey)
		if ok {
			if len(val) == 0 {
				return nil, nil
			}
			return t.expandBlob(val)
		}
	}

	// 3. Check flexfile (table's own tree/cache).
	h2 := hash32(key)
	lockID = t.enterFF(h2)
	val := t.getPassthrough(key, ref.itvbuf, fp16(h2))
	t.exitFF(lockID)
	if len(val) == 0 {
		return nil, nil
	}
	return t.expandBlob(val)
}

// Probe checks whether a key exists. Returns false for tombstones.
func (ref *TableRef) Probe(key []byte) (bool, error) {
	t := ref.table
	db := t.db
	pkey := makePrefixedKey(t.id, key)
	h := hash32(pkey)

	lockID := db.enterMT(h)
	active := db.activeMT()
	if !active.hidden.Load() {
		p := active.probe(pkey)
		db.exitMT(lockID)
		if p != 0 {
			return p == 2, nil
		}
	} else {
		db.exitMT(lockID)
	}

	immut := db.immutableMT()
	if !immut.hidden.Load() {
		p := immut.probe(pkey)
		if p != 0 {
			return p == 2, nil
		}
	}

	h2 := hash32(key)
	lockID = t.enterFF(h2)
	found := t.probePassthrough(key, ref.itvbuf, fp16(h2))
	t.exitFF(lockID)
	return found, nil
}

// Delete removes a key by inserting a tombstone.
func (ref *TableRef) Delete(key []byte) error {
	return ref.Put(key, nil)
}

// Sync flushes all pending writes. Returns the first flush error, if any.
func (ref *TableRef) Sync() error {
	return ref.table.db.Sync()
}

// NewIterator creates a positioned iterator scoped to this table.
func (ref *TableRef) NewIterator() *Iterator {
	t := ref.table
	it := &Iterator{
		table:        t,
		itvbuf:       ref.itvbuf,
		tableIDBytes: tableIDPrefix(t.id),
	}
	it.current = &it.currentKV
	return it
}

// Update atomically compares the current value of key with oldValue and, if
// they match, replaces it with newValue. Returns (true, nil) on success,
// (false, nil) if the current value did not match.
//
// Pass nil oldValue to require the key to be absent.
// Pass nil newValue to delete the key when the match succeeds.
func (ref *TableRef) Update(key, oldValue, newValue []byte) (bool, error) {
	t := ref.table
	db := t.db
	if len(key) == 0 || len(key) > MaxKVSize {
		return false, errInvalidKV
	}
	if len(newValue) > MaxBlobSize {
		return false, errBlobTooLarge
	}

	pkey := makePrefixedKey(t.id, key)
	h := hash32(pkey)
	lockID := h & (lockShards - 1)

	// Spin until the active memtable has capacity (same as Put).
	for db.activeMT().isFull() {
	}

	// Hold the write lock for this MT shard for the entire check+write.
	// Because a memtable swap requires all 16 write locks, the active MT
	// cannot change while we hold this one lock — no hidden-flag check needed.
	db.rwMT[lockID].Lock()
	defer db.rwMT[lockID].Unlock()

	// Read current raw value: active MT → immutable MT → flexfile.
	var rawCur []byte
	found := false
	if val, ok := db.activeMT().get(pkey); ok {
		rawCur, found = val, true
	} else if val, ok := db.immutableMT().get(pkey); ok {
		rawCur, found = val, true
	}
	if found {
		if len(rawCur) == 0 {
			rawCur = nil // tombstone → treat as absent
		}
	} else {
		// Fall back to the flexfile (rwMT → rwFF: same order as the flush path).
		h2 := hash32(key)
		ffLockID := h2 & (lockShards - 1)
		t.rwFF[ffLockID].RLock()
		rawCur = t.getPassthrough(key, ref.itvbuf, fp16(h2))
		t.rwFF[ffLockID].RUnlock()
		if len(rawCur) == 0 {
			rawCur = nil
		}
	}

	// Expand blob sentinel for comparison.
	cur := rawCur
	if cur != nil && isBlobSentinel(cur) {
		expanded, err := t.expandBlob(cur)
		if err != nil {
			return false, err
		}
		cur = expanded
	}

	if !bytes.Equal(cur, oldValue) {
		return false, nil
	}

	// Determine what to store for newValue.
	storeValue := newValue
	if len(key)+len(newValue) > MaxKVSize && len(newValue) > 0 {
		offset, err := t.blobs.write(newValue)
		if err != nil {
			return false, err
		}
		sentinel := make([]byte, blobSentinelSize)
		encodeBlobSentinel(sentinel, offset, uint32(len(newValue)))
		storeValue = sentinel
	}

	seq := db.seqNum.Add(1)
	if err := db.activeMT().put(pkey, storeValue, seq); err != nil {
		return false, err
	}
	return true, nil
}

var (
	errInvalidKV    = errors.New("flexdb: invalid KV (empty key or key exceeds MaxKVSize)")
	errBlobTooLarge = errors.New("flexdb: value exceeds MaxBlobSize")
)
