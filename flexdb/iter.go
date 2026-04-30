package flexdb

import (
	"encoding/binary"
)

// Iterator performs a sorted forward scan over all live key-value pairs in a
// single Table. It merges three sources — the active memtable, the immutable
// (flushing) memtable, and the on-disk flexfile — using a 3-way merge.
// Tombstones are suppressed. Memtable data takes precedence over file data.
//
// Blob values (values stored outside the main KV store) are transparently
// expanded during iteration. If a blob read fails, iteration stops and the
// error is available via Err().
//
// The iterator is scoped to one table: memtable keys are filtered by their
// tableID prefix (first 4 bytes), which is stripped before keys are returned.
// File keys are stored without a prefix (it is stripped at flush time).
//
// Create one per goroutine with TableRef.NewIterator. Always call Close when done.
type Iterator struct {
	table *Table

	// Pinned memtables to prevent clear during iteration.
	pinned [2]*memtable

	// File iterator state (uses table.tree/cache directly; keys have no prefix).

	fileNode    *treeNode
	fileAncIdx  int
	fileKVIdx   int
	fileShift   int64
	fileCE      *cacheEntry
	fileCP      *cachePartition
	fileCurrent *KV

	// Memtable iterators (index 0 = active, 1 = immutable).
	// Keys in the memtable are tableID-prefixed; we compare stripped user keys.
	mtIters [2]*skipIter
	mtOK    [2]bool

	current   *KV
	currentKV KV
	valid     bool
	err       error // set if a blob read fails; cleared on Seek

	// tableIDBytes is the 4-byte big-endian tableID prefix for filtering.
	tableIDBytes [4]byte

	// Scratch buffer for loading cache entries.
	itvbuf []byte
}

// Seek positions the iterator at the first key >= key.
// A nil key seeks to the very first entry.
func (it *Iterator) Seek(key []byte) {
	t := it.table
	db := t.db
	it.valid = false
	it.err = nil
	it.currentKV.Key = nil
	it.currentKV.Value = nil

	it.unpinMemtables()

	// Seek file iterator (table-scoped, no prefix).
	it.seekFile(key)

	// Build prefixed start key for memtable seek.
	// When key is non-nil, borrow a pooled buffer (Seek does not retain the key).
	var pkey []byte
	var pkeyBuf *[]byte // non-nil when pooled; must be returned after seeks complete
	if key == nil {
		pkey = it.tableIDBytes[:]
	} else {
		pkeyBuf = pkeyPool.Get().(*[]byte)
		pkey = (*pkeyBuf)[:4+len(key)]
		copy(pkey, it.tableIDBytes[:])
		copy(pkey[4:], key)
	}

	// Snapshot and pin current memtables.
	active := db.activeMT()
	immut := db.immutableMT()
	it.pinned[0] = active
	it.pinned[1] = immut
	active.readers.Add(1)
	immut.readers.Add(1)

	// Seek active memtable.
	if !active.hidden.Load() {
		si := active.m.Seek(pkey)
		it.mtIters[0] = si
		it.mtOK[0] = si.Valid() && it.matchesTableID(si.Key())
	} else {
		it.mtIters[0] = nil
		it.mtOK[0] = false
	}

	// Seek immutable memtable.
	if !immut.hidden.Load() {
		si := immut.m.Seek(pkey)
		it.mtIters[1] = si
		it.mtOK[1] = si.Valid() && it.matchesTableID(si.Key())
	} else {
		it.mtIters[1] = nil
		it.mtOK[1] = false
	}

	// Return the pooled pkey buffer now that all seeks are done.
	if pkeyBuf != nil {
		pkeyPool.Put(pkeyBuf)
		pkeyBuf = nil
	}

	it.advance()
}

// matchesTableID reports whether key starts with the iterator's tableID prefix.
func (it *Iterator) matchesTableID(key []byte) bool {
	if len(key) < 4 {
		return false
	}
	return key[0] == it.tableIDBytes[0] &&
		key[1] == it.tableIDBytes[1] &&
		key[2] == it.tableIDBytes[2] &&
		key[3] == it.tableIDBytes[3]
}

// seekFile positions the file iterator at the first key >= key.
func (it *Iterator) seekFile(key []byte) {
	t := it.table
	it.releaseFileCE()

	var seekKey []byte
	if key == nil {
		seekKey = []byte{}
	} else {
		seekKey = key
	}

	nh := t.tree.findAnchorPos(seekKey)
	it.fileNode = nh.node
	it.fileShift = nh.shift
	it.fileAncIdx = int(nh.idx)

	a := nh.node.anchors[nh.idx]
	loff := uint64(a.loff) + uint64(nh.shift)

	p := t.cache.partitionFor(a)
	e := p.getEntry(a, loff, it.itvbuf)
	it.fileCE = e
	it.fileCP = p

	if len(seekKey) == 0 {
		it.fileKVIdx = 0
	} else {
		pos := e.findKeyGE(seekKey, fp16(hash32(seekKey)))
		it.fileKVIdx = pos &^ (1 << 31)
	}
	it.fileCurrentFromCache()
}

func (it *Iterator) fileCurrentFromCache() {
	e := it.fileCE
	if e == nil || it.fileKVIdx >= e.count {
		it.fileCurrent = nil
		return
	}
	it.fileCurrent = e.kvs[it.fileKVIdx]
}

func (it *Iterator) releaseFileCE() {
	if it.fileCE != nil && it.fileCP != nil {
		it.fileCP.releaseEntry(it.fileCE)
		it.fileCE = nil
		it.fileCP = nil
	}
}

func (it *Iterator) fileNext() {
	if it.fileCE == nil {
		it.fileCurrent = nil
		return
	}
	it.fileKVIdx++
	if it.fileKVIdx < it.fileCE.count {
		it.fileCurrentFromCache()
		return
	}
	it.releaseFileCE()

	it.fileAncIdx++
	if it.fileAncIdx >= int(it.fileNode.count) {
		it.fileNode = it.fileNode.next
		it.fileAncIdx = 0
		if it.fileNode != nil {
			nh := nodeHandler{node: it.fileNode, idx: 0}
			accumulatedShift(&nh)
			it.fileShift = nh.shift
		}
	}
	if it.fileNode == nil || it.fileAncIdx >= int(it.fileNode.count) {
		it.fileCurrent = nil
		return
	}

	a := it.fileNode.anchors[it.fileAncIdx]
	loff := uint64(a.loff) + uint64(it.fileShift)
	t := it.table
	p := t.cache.partitionFor(a)
	e := p.getEntry(a, loff, it.itvbuf)
	it.fileCE = e
	it.fileCP = p
	it.fileKVIdx = 0
	it.fileCurrentFromCache()
}

// advance merges three sources, sets it.current, and skips tombstones.
// Memtable keys have a 4-byte tableID prefix; we compare user keys (key[4:]).
func (it *Iterator) advance() {
	for {
		minKey, minSrc := it.findMin()
		if minKey == nil {
			it.valid = false
			return
		}

		var rawValue []byte
		switch minSrc {
		case 0, 1: // memtable — strip the 4-byte tableID prefix
			si := it.mtIters[minSrc]
			it.currentKV.Key = si.Key()[4:]
			rawValue = si.Value()
		default: // file — keys already have no prefix
			it.currentKV.Key = it.fileCurrent.Key
			rawValue = it.fileCurrent.Value
		}

		it.skipKey(minKey)

		if len(rawValue) == 0 {
			continue // tombstone
		}

		// Expand blob sentinel transparently.
		if isBlobSentinel(rawValue) {
			offset, size := decodeBlobSentinel(rawValue)
			expanded, err := it.table.blobs.read(offset, size)
			if err != nil {
				it.err = err
				it.valid = false
				return
			}
			it.currentKV.Value = expanded
		} else {
			it.currentKV.Value = rawValue
		}

		it.valid = true
		return
	}
}

// findMin returns the minimum user key and its source (0/1=memtable, 2=file).
func (it *Iterator) findMin() ([]byte, int) {
	// Fast path: determine how many sources are active and skip comparisons
	// when only one source remains — the common case near end of iteration.
	mt0 := it.mtOK[0]
	mt1 := it.mtOK[1]
	hasFile := it.fileCurrent != nil

	switch {
	case !mt0 && !mt1 && !hasFile:
		return nil, 2
	case !mt0 && !mt1:
		return it.fileCurrent.Key, 2
	case !mt1 && !hasFile:
		return it.mtIters[0].Key()[4:], 0
	case !mt0 && !hasFile:
		return it.mtIters[1].Key()[4:], 1
	}

	// Multiple sources active — full comparison needed.
	var minKey []byte
	src := 2

	for i := range 2 {
		if !it.mtOK[i] {
			continue
		}
		k := it.mtIters[i].Key()[4:] // strip tableID prefix
		if minKey == nil || compareKeys(k, minKey) < 0 {
			minKey = k
			src = i
		}
	}
	if hasFile {
		k := it.fileCurrent.Key
		if minKey == nil || compareKeys(k, minKey) < 0 {
			minKey = k
			src = 2
		}
	}
	return minKey, src
}

// skipKey advances all sources that are positioned at userKey.
func (it *Iterator) skipKey(userKey []byte) {
	for i := range 2 {
		if it.mtOK[i] && compareKeys(it.mtIters[i].Key()[4:], userKey) == 0 {
			it.mtIters[i].Next()
			it.mtOK[i] = it.mtIters[i].Valid() && it.matchesTableID(it.mtIters[i].Key())
		}
	}
	if it.fileCurrent != nil && compareKeys(it.fileCurrent.Key, userKey) == 0 {
		it.fileNext()
	}
}

// Valid returns true when the iterator is at a valid key.
func (it *Iterator) Valid() bool { return it.valid }

// Err returns the first error encountered during iteration, if any.
// A non-nil error indicates that iteration stopped early (e.g., a blob read
// failed). Always check Err after a loop that exits because Valid() is false.
func (it *Iterator) Err() error { return it.err }

// Current returns the current KV (user key, no tableID prefix). Valid only when Valid()==true.
func (it *Iterator) Current() *KV { return it.current }

// Next advances to the next key.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	it.advance()
}

// Close releases iterator resources.
func (it *Iterator) Close() {
	it.releaseFileCE()
	it.unpinMemtables()
	it.valid = false
	it.currentKV.Key = nil
	it.currentKV.Value = nil
}

func (it *Iterator) unpinMemtables() {
	for i := range it.pinned {
		if it.pinned[i] != nil {
			it.pinned[i].readers.Add(-1)
			it.pinned[i] = nil
		}
	}
}

// tableIDPrefix builds the 4-byte big-endian prefix for tableID.
func tableIDPrefix(id uint32) [4]byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], id)
	return b
}
