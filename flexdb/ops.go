package flexdb

import (
	"bytes"
	"errors"
)

// Put inserts or updates a key-value pair in the table.
// Caller must not hold any DB locks.
func (ref *TableRef) Put(key, value []byte) error {
	t := ref.table
	db := t.db
	if len(key) == 0 || len(key)+len(value) > MaxKVSize {
		return errInvalidKV
	}

	pkey := makePrefixedKey(t.id, key)
	h := hash32(pkey)

	for db.activeMT().isFull() {
		// Brief yield; background worker swaps when ready.
	}

	seq := db.seqNum.Add(1)
	lockID := db.enterMT(h)
	db.activeMT().put(pkey, value, seq)
	db.exitMT(lockID)
	return nil
}

// Get returns a copy of the value for key, or nil if not found.
func (ref *TableRef) Get(key []byte) ([]byte, error) {
	val, err := ref.GetView(key)
	if err != nil || val == nil {
		return nil, err
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// GetView returns a direct view of the value for key. The returned slice is
// only valid until the next mutating operation on the DB.
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
			return val, nil
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
			return val, nil
		}
	}

	// 3. Check flexfile (table's own tree/cache).
	h2 := hash32(key)
	lockID = t.enterFF(h2)
	val := t.getPassthrough(key, ref.itvbuf, fp16(h2))
	t.exitFF(lockID)
	if val == nil || len(val) == 0 {
		return nil, nil
	}
	return val, nil
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

// Sync flushes all pending writes.
func (ref *TableRef) Sync() {
	ref.table.db.Sync()
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
	if len(key) == 0 {
		return false, errInvalidKV
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

	// Read current value: active MT → immutable MT → flexfile.
	var cur []byte
	found := false
	if val, ok := db.activeMT().get(pkey); ok {
		cur, found = val, true
	} else if val, ok := db.immutableMT().get(pkey); ok {
		cur, found = val, true
	}
	if found {
		if len(cur) == 0 {
			cur = nil // tombstone → treat as absent
		}
	} else {
		// Fall back to the flexfile (rwMT → rwFF: same order as the flush path).
		h2 := hash32(key)
		ffLockID := h2 & (lockShards - 1)
		t.rwFF[ffLockID].RLock()
		cur = t.getPassthrough(key, ref.itvbuf, fp16(h2))
		t.rwFF[ffLockID].RUnlock()
		if len(cur) == 0 {
			cur = nil
		}
	}

	if !bytes.Equal(cur, oldValue) {
		return false, nil
	}

	seq := db.seqNum.Add(1)
	db.activeMT().put(pkey, newValue, seq)
	return true, nil
}

var errInvalidKV = errors.New("flexdb: invalid KV (empty key or size exceeds MaxKVSize)")
