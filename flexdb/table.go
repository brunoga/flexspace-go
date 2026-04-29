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

	"github.com/brunoga/flexspace-go/flexfile"
)

// Table is a named, isolated key-value namespace within a DB.
// Each Table has its own on-disk flexfile, enabling O(1) DropTable.
// Access data via Table.NewRef (one Ref per goroutine).
type Table struct {
	db   *DB
	id   uint32
	name string

	ff    Storage
	tree  *dbTree
	cache *dbCache
	blobs *blobStore // append-only blob file for values exceeding MaxKVSize

	// Per-table flexfile read-write locks (16 shards).
	rwFF [lockShards]sync.RWMutex

	// Scratch buffers used exclusively in the flush path (protected by rwFF write locks).
	kvbuf  []byte
	itvbuf []byte
}

// TableRef is a per-goroutine handle for Table operations.
// Must not be shared across goroutines.
type TableRef struct {
	table  *Table
	itvbuf []byte // for loading cache entries
}

// NewRef creates a per-goroutine handle for this table.
func (t *Table) NewRef() *TableRef {
	return &TableRef{
		table:  t,
		itvbuf: make([]byte, (sparseInterval*MaxKVSize)+MaxKVSize),
	}
}

// tableDir returns the directory path for a given table ID.
func tableDir(dbPath string, id uint32) string {
	return filepath.Join(dbPath, "tables", fmt.Sprintf("%d", id))
}

// openTable opens or creates a table with the given ID and name.
func (db *DB) openTable(ctx context.Context, id uint32, name string) (*Table, error) {
	dir := tableDir(db.path, id)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("openTable %q: mkdir: %w", name, err)
	}

	ff, err := openStorage(filepath.Join(dir, "FLEXFILE"))
	if err != nil {
		return nil, fmt.Errorf("openTable %q: flexfile: %w", name, err)
	}
	blobs, err := openBlobStore(dir)
	if err != nil {
		ff.Close()
		return nil, fmt.Errorf("openTable %q: blobstore: %w", name, err)
	}
	t := &Table{
		db:     db,
		id:     id,
		name:   name,
		ff:     ff,
		blobs:  blobs,
		kvbuf:  make([]byte, MaxKVSize*2),
		itvbuf: make([]byte, (sparseInterval*MaxKVSize)+MaxKVSize),
	}
	t.tree = newDBTree()
	t.cache = newDBCache(db, ff, db.capMB)

	ff.SetMetrics(flexfile.Metrics{
		WALWriteCount:    &db.metrics.WALWriteCount,
		GCReclaimedBytes: &db.metrics.GCReclaimedBytes,
		GCMovedBytes:     &db.metrics.GCMovedBytes,
	})

	if ff.Size() == 0 {

		if err := t.initSentinel(); err != nil {
			ff.Close()
			blobs.close()
			return nil, fmt.Errorf("openTable %q: init sentinel: %w", name, err)
		}
	} else {
		select {
		case <-ctx.Done():
			ff.Close()
			blobs.close()
			return nil, ctx.Err()
		default:
		}
		if err := t.rebuildIndex(); err != nil {
			ff.Close()
			blobs.close()
			return nil, fmt.Errorf("openTable %q: rebuild index: %w", name, err)
		}
	}
	return t, nil
}

func (t *Table) close() error {
	blobErr := t.blobs.close()
	ffErr := t.ff.Close()
	if blobErr != nil {
		return blobErr
	}
	return ffErr
}

// expandBlob dereferences val if it is a blob sentinel, returning the actual
// blob bytes. For inline values, val is returned unchanged. An I/O error is
// returned only when dereferencing a sentinel fails.
func (t *Table) expandBlob(val []byte) ([]byte, error) {
	if !isBlobSentinel(val) {
		return val, nil
	}
	offset, size := decodeBlobSentinel(val)
	return t.blobs.read(offset, size)
}

// TableStats holds statistics for a single flexdb table.
type TableStats struct {
	FileBytes   uint64 // logical bytes on disk
	AnchorCount int    // number of leaf anchors (sorted key intervals in the B+tree)
	CacheUsed   uint64 // cache bytes currently in use
	CacheCap    uint64 // cache byte capacity
}

// Stats returns a snapshot of the table's internal statistics.
func (t *Table) Stats() TableStats {
	used, cap := t.cache.stats()
	return TableStats{
		FileBytes:   t.ff.Size(),
		AnchorCount: t.tree.anchorCount(),
		CacheUsed:   used,
		CacheCap:    cap,
	}
}

func (t *Table) initSentinel() error {
	// The file is empty at this point; SetTag has no extent to tag yet.
	// The anchor tag at offset 0 is applied when the first KV is written
	// via putPassthroughUnsorted. The call here is intentionally best-effort.
	tag := fileTagGenerate(true, 0)
	_ = t.ff.SetTag(0, tag)
	return nil
}

// rebuildIndex scans the table's flexfile and rebuilds the in-memory anchor tree.
func (t *Table) rebuildIndex() error {
	totalSize := t.ff.Size()
	if totalSize == 0 {
		return nil
	}

	nWorkers := 4
	chunkSize := totalSize / uint64(nWorkers)
	if chunkSize == 0 {
		nWorkers = 1
		chunkSize = totalSize
	}

	type result struct {
		anchors []recoveryAnchor
		err     error
	}
	results := make([]result, nWorkers)

	var wg sync.WaitGroup
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		start := uint64(i) * chunkSize
		end := start + chunkSize
		if i == nWorkers-1 {
			end = totalSize
		}
		go func(workerIdx int, start, end uint64) {
			defer wg.Done()
			var anchors []recoveryAnchor
			err := t.ff.IterateExtents(start, end, func(loff uint64, tag uint16, data []byte) bool {
				isAnchor := fileTagIsAnchor(tag)
				if isAnchor {
					kv, _ := decodeKV128(data)
					if kv == nil {
						return true
					}
					var key []byte
					if loff > 0 {
						key = make([]byte, len(kv.Key))
						copy(key, kv.Key)
					} else {
						key = []byte{}
					}
					anchors = append(anchors, recoveryAnchor{
						key:      key,
						loff:     loff,
						unsorted: fileTagUnsorted(tag),
					})
				}
				return true
			})
			results[workerIdx].anchors = anchors
			results[workerIdx].err = err
		}(i, start, end)
	}
	wg.Wait()

	var allAnchors []recoveryAnchor
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		allAnchors = append(allAnchors, r.anchors...)
	}

	sort.Slice(allAnchors, func(i, j int) bool {
		return allAnchors[i].loff < allAnchors[j].loff
	})

	for i, ra := range allAnchors {
		var psize uint32
		if i+1 < len(allAnchors) {
			psize = uint32(allAnchors[i+1].loff - ra.loff)
		} else {
			psize = uint32(totalSize - ra.loff)
		}
		if ra.loff == 0 && len(ra.key) == 0 {
			a := t.tree.leafHead.anchors[0]
			a.psize = psize
			a.unsorted = ra.unsorted
			continue
		}
		nh := t.tree.findAnchorPos(ra.key)
		t.tree.insertAnchor(&nh, ra.key, ra.loff, psize)
	}
	return nil
}

// enterFF acquires a read lock on the shard for hash.
func (t *Table) enterFF(hash uint32) uint32 {
	id := hash & (lockShards - 1)
	t.rwFF[id].RLock()
	return id
}

func (t *Table) exitFF(id uint32) {
	t.rwFF[id].RUnlock()
}

// ---- passthrough operations (called under table FF lock) ----

func (t *Table) getPassthrough(key, itvbuf []byte, fp uint16) []byte {
	nh := t.tree.findAnchorPos(key)

	a := nh.node.anchors[nh.idx]
	loff := uint64(a.loff) + uint64(nh.shift)

	p := t.cache.partitionFor(a)
	e := p.getEntry(a, loff, itvbuf)
	defer p.releaseEntry(e)

	i := e.findKeyEQ(key, fp)
	if i >= sparseInterval || i >= e.count {
		return nil
	}
	return e.kvs[i].Value
}

func (t *Table) probePassthrough(key, itvbuf []byte, fp uint16) bool {
	nh := t.tree.findAnchorPos(key)

	a := nh.node.anchors[nh.idx]
	loff := uint64(a.loff) + uint64(nh.shift)

	p := t.cache.partitionFor(a)
	e := p.getEntry(a, loff, itvbuf)
	defer p.releaseEntry(e)

	i := e.findKeyEQ(key, fp)
	if i >= sparseInterval || i >= e.count {
		return false
	}
	return len(e.kvs[i].Value) > 0
}

func (t *Table) putPassthrough(kv *KV, nh *nodeHandler) error {
	key := kv.Key
	t.tree.nextAnchor(nh, key)

	a := nh.node.anchors[nh.idx]
	loff := uint64(a.loff) + uint64(nh.shift)

	p := t.cache.partitionFor(a)
	e := p.getEntryUnsorted(a, loff, t.itvbuf)

	if e == nil {
		return t.putPassthroughUnsorted(kv, nh, a)
	}

	if a.unsorted > 0 {
		if err := t.rewriteInterval(nh, a, e); err != nil {
			p.releaseEntry(e)
			return err
		}
		e.clearFrag()
	}

	if err := t.putPassthroughCached(kv, nh, a, p, e); err != nil {
		p.releaseEntry(e)
		return err
	}

	if e.frag {
		if err := t.rewriteInterval(nh, a, e); err != nil {
			p.releaseEntry(e)
			return err
		}
		e.clearFrag()
	}

	if e.count >= sparseIntervalCount || a.psize >= sparseIntervalSize {
		if err := t.splitAnchor(nh, p, e); err != nil {
			p.releaseEntry(e)
			return err
		}
	}

	p.releaseEntry(e)
	return nil
}

func (t *Table) putPassthroughUnsorted(kv *KV, nh *nodeHandler, a *anchor) error {
	anchorLoff := uint64(a.loff) + uint64(nh.shift)

	n := encodeKV128(t.kvbuf, kv)
	psize := uint32(n)
	loff := anchorLoff + uint64(a.psize)

	if _, err := t.ff.Insert(t.kvbuf[:n], loff); err != nil {
		return fmt.Errorf("flexdb: Insert failed: %w", err)
	}
	shiftUpPropagate(nh, int64(psize))

	a.psize += psize
	a.unsorted++

	tag := fileTagGenerate(true, a.unsorted)
	if err := t.ff.SetTag(anchorLoff, tag); err != nil {
		return fmt.Errorf("flexdb: SetTag failed: %w", err)
	}

	if nh.node.parent != nil {
		rebaseLeaf(nh.node)
	}
	return nil
}

func (t *Table) putPassthroughCached(kv *KV, nh *nodeHandler, a *anchor, p *cachePartition, e *cacheEntry) error {
	anchorLoff := uint64(a.loff) + uint64(nh.shift)

	newKV := dupKV(kv)
	newSize := uint64(kv128EncodedSize(len(kv.Key), len(kv.Value)))

	p.mu.Lock()
	p.size += newSize
	p.calibrate(0)
	p.mu.Unlock()

	pos := e.findKeyGE(kv.Key, fp16(hash32(kv.Key)))
	isUpdate := (pos & (1 << 31)) != 0
	idx := pos &^ (1 << 31)

	loff := anchorLoff
	for j := range idx {
		loff += uint64(kv128EncodedSize(len(e.kvs[j].Key), len(e.kvs[j].Value)))
	}

	n := encodeKV128(t.kvbuf, newKV)
	psize := uint32(n)
	if isUpdate {
		oldPsize := uint32(kv128EncodedSize(len(e.kvs[idx].Key), len(e.kvs[idx].Value)))
		if _, err := t.ff.Update(t.kvbuf[:n], loff, uint64(oldPsize)); err != nil {
			// Undo the p.size pre-adjustment so the cache size stays correct.
			p.mu.Lock()
			p.size -= newSize
			p.mu.Unlock()
			slog.Error("putPassthroughCached: Update failed", "key", string(kv.Key), "loff", loff, "oldPsize", oldPsize, "anchorLoff", anchorLoff, "idx", idx, "count", e.count, "err", err)
			return fmt.Errorf("flexdb: Update failed: %w", err)
		}
		e.replaceKV(p, newKV, idx)
		if psize != oldPsize {
			shiftUpPropagate(nh, int64(psize)-int64(oldPsize))
		}
		a.psize = uint32(int32(a.psize) + int32(psize) - int32(oldPsize))
	} else {
		if idx == 0 {
			if _, err := t.ff.Insert(t.kvbuf[:n], loff); err != nil {
				p.mu.Lock()
				p.size -= newSize
				p.mu.Unlock()
				slog.Error("putPassthroughCached: Insert(idx=0) failed", "key", string(kv.Key), "loff", loff, "anchorLoff", anchorLoff, "idx", idx, "count", e.count, "err", err)
				return fmt.Errorf("flexdb: Insert failed: %w", err)
			}
			e.insertKV(p, newKV, idx)
			tag := fileTagGenerate(true, a.unsorted)
			if err := t.ff.SetTag(loff, tag); err != nil {
				return fmt.Errorf("flexdb: SetTag failed: %w", err)
			}
			// Clear the anchor bit from the old first extent (now shifted forward).
			_ = t.ff.SetTag(loff+uint64(n), 0)
		} else {
			if _, err := t.ff.Insert(t.kvbuf[:n], loff); err != nil {
				p.mu.Lock()
				p.size -= newSize
				p.mu.Unlock()
				slog.Error("putPassthroughCached: Insert failed", "key", string(kv.Key), "loff", loff, "anchorLoff", anchorLoff, "idx", idx, "count", e.count, "err", err)
				return fmt.Errorf("flexdb: Insert failed: %w", err)
			}
			e.insertKV(p, newKV, idx)
		}
		shiftUpPropagate(nh, int64(psize))
		a.psize += psize
	}

	if nh.node.parent != nil {
		rebaseLeaf(nh.node)
	}
	return nil
}

func (t *Table) rewriteInterval(nh *nodeHandler, a *anchor, e *cacheEntry) error {
	anchorLoff := uint64(a.loff) + uint64(nh.shift)

	off := 0
	for i := 0; i < e.count; i++ {
		n := encodeKV128(t.itvbuf[off:], e.kvs[i])
		off += n
	}
	newPsize := uint32(off)
	if _, err := t.ff.Update(t.itvbuf[:off], anchorLoff, uint64(a.psize)); err != nil {
		return fmt.Errorf("flexdb: Update failed: %w", err)
	}
	if newPsize != a.psize {
		shiftUpPropagate(nh, int64(newPsize)-int64(a.psize))
	}
	a.psize = newPsize
	a.unsorted = 0
	tag := fileTagGenerate(true, 0)
	if err := t.ff.SetTag(anchorLoff, tag); err != nil {
		return fmt.Errorf("flexdb: SetTag failed: %w", err)
	}
	return nil
}

func (t *Table) splitAnchor(nh *nodeHandler, p *cachePartition, e *cacheEntry) error {
	a := nh.node.anchors[nh.idx]
	anchorLoff := uint64(a.loff) + uint64(nh.shift)

	count := e.count
	rightCount := count / 2
	leftCount := count - rightCount

	var leftPsize uint32
	for i := range leftCount {
		leftPsize += uint32(kv128EncodedSize(len(e.kvs[i].Key), len(e.kvs[i].Value)))
	}

	// Compute rightPsize from cache entries (ground truth), not from a.psize
	// which may be stale (e.g., after WAL replay when rebuildIndex assigns psizes
	// from phase-1 flexfile anchor distances that differ from cache reality).
	// Using a stale a.psize here would create an anchor with a wrong psize,
	// causing loadEntry to read past the anchor's actual data region and
	// decode garbage bytes as cache entries, cascading into further corruption.
	var rightPsize uint32
	for i := leftCount; i < count; i++ {
		rightPsize += uint32(kv128EncodedSize(len(e.kvs[i].Key), len(e.kvs[i].Value)))
	}
	rightLoff := anchorLoff + uint64(leftPsize)

	newKey := dupKey(e.kvs[leftCount].Key)
	newAnchor := t.tree.insertAnchor(nh, newKey, rightLoff, rightPsize)

	newP := t.cache.partitionFor(newAnchor)
	newE := &cacheEntry{anchor: newAnchor}
	newAnchor.cacheEntry = newE
	newE.refcnt = 1
	newE.access = cacheEntryChance

	var rightSize uint32
	for i := range rightCount {
		newE.kvs[i] = e.kvs[leftCount+i]
		newE.fps[i] = e.fps[leftCount+i]
		rightSize += uint32(kv128EncodedSize(len(newE.kvs[i].Key), len(newE.kvs[i].Value)))
	}
	newE.count = rightCount
	newE.size = rightSize
	newE.frag = e.frag

	newP.mu.Lock()
	newP.linkEntry(newE)
	if p != newP {
		newP.size += uint64(rightSize)
	}
	newP.mu.Unlock()

	var leftSize uint32
	for i := range leftCount {
		leftSize += uint32(kv128EncodedSize(len(e.kvs[i].Key), len(e.kvs[i].Value)))
	}
	e.count = leftCount
	e.size = leftSize
	a.psize = leftPsize

	if p != newP {
		p.mu.Lock()
		p.size -= uint64(rightSize)
		p.mu.Unlock()
	}

	tag := fileTagGenerate(true, 0)
	if err := t.ff.SetTag(rightLoff, tag); err != nil {
		newP.releaseEntry(newE)
		return fmt.Errorf("flexdb: SetTag failed: %w", err)
	}

	newP.releaseEntry(newE)
	return nil
}

func (t *Table) deletePassthrough(key []byte, nh *nodeHandler, fp uint16) error {
	t.tree.nextAnchor(nh, key)

	a := nh.node.anchors[nh.idx]
	anchorLoff := uint64(a.loff) + uint64(nh.shift)

	p := t.cache.partitionFor(a)
	e := p.getEntry(a, anchorLoff, t.itvbuf)
	defer p.releaseEntry(e)

	if a.unsorted > 0 {
		if err := t.rewriteInterval(nh, a, e); err != nil {
			return err
		}
		e.clearFrag()
	}

	loff := anchorLoff
	i := e.findKeyEQ(key, fp)
	if i >= sparseInterval || i >= e.count {
		return nil
	}

	for j := range i {
		loff += uint64(kv128EncodedSize(len(e.kvs[j].Key), len(e.kvs[j].Value)))
	}
	entryPsize := uint32(kv128EncodedSize(len(e.kvs[i].Key), len(e.kvs[i].Value)))

	e.deleteKV(p, i)

	if i == 0 && e.count > 1 {
		if err := t.ff.Collapse(loff, uint64(entryPsize)); err != nil {
			return fmt.Errorf("flexdb: Collapse failed: %w", err)
		}
		tag := fileTagGenerate(true, a.unsorted)
		if err := t.ff.SetTag(loff, tag); err != nil {
			return fmt.Errorf("flexdb: SetTag failed: %w", err)
		}
	} else {
		if err := t.ff.Collapse(loff, uint64(entryPsize)); err != nil {
			return fmt.Errorf("flexdb: Collapse failed: %w", err)
		}
	}
	shiftUpPropagate(nh, -int64(entryPsize))
	a.psize -= entryPsize

	if i == 0 && len(a.key) > 0 {
		t.deleteUpdateTree(nh, e)
	}

	if e.frag && a.psize > 0 {
		if err := t.rewriteInterval(nh, a, e); err != nil {
			return err
		}
		e.clearFrag()
	}
	return nil
}

func (t *Table) deleteUpdateTree(nh *nodeHandler, e *cacheEntry) {
	node := nh.node
	a := node.anchors[nh.idx]

	if a.psize == 0 {
		t.tree.removeAnchorAt(nh)
		nh.node = nil
		return
	}

	if e.count > 0 {
		a.key = dupKey(e.kvs[0].Key)
		a.hash = hash32(a.key)
		if nh.idx == 0 {
			updateSmallestKey(node, a.key)
		}
	}
	nh.node = nil
}

const (
	// sparseIntervalCount is the maximum KV entries per anchor before splitting.
	sparseIntervalCount = 16
	// sparseIntervalSize is the maximum physical byte size of an anchor interval before splitting.
	sparseIntervalSize = 16384
)

// makePrefixedKey builds the 4-byte big-endian tableID prefix followed by the user key.
// The returned slice is a fresh allocation owned by the caller (safe to store in skip list).
func makePrefixedKey(tableID uint32, key []byte) []byte {
	pkey := make([]byte, 4+len(key))
	binary.BigEndian.PutUint32(pkey, tableID)
	copy(pkey[4:], key)
	return pkey
}

// pkeyPool recycles prefixed-key buffers for read-path operations that do NOT
// retain the key (GetView, Probe, Iterator.Seek, Batch.readCurrent).
// Must NOT be used for Put/Update since those paths store the key in the skip list.
var pkeyPool = sync.Pool{
	New: func() any { return make([]byte, 4+MaxKVSize) },
}

// borrowPKey returns a pooled buffer containing tableID prefix + key.
// The caller MUST call returnPKey when done; after that the slice is invalid.
func borrowPKey(tableID uint32, key []byte) []byte {
	buf := pkeyPool.Get().([]byte)
	pkey := buf[:4+len(key)]
	binary.BigEndian.PutUint32(pkey, tableID)
	copy(pkey[4:], key)
	return pkey
}

// returnPKey returns a slice obtained from borrowPKey to the pool.
func returnPKey(pkey []byte) {
	pkeyPool.Put(pkey[:cap(pkey)])
}
