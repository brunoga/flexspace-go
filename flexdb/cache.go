package flexdb

import (
	"encoding/binary"
	"sync"
	"unsafe"
)

// Cache constants matching the C implementation.
const (
	cachePartitionBits  = 10
	cachePartitionCount = 1 << cachePartitionBits // 1024
	cachePartitionMask  = cachePartitionCount - 1

	sparseInterval = 32 // FLEXDB_TREE_SPARSE_INTERVAL (16 sorted + 15 unsorted + 1)

	cacheEntryChance = 2 // second-chance budget for replacement
)

// cacheEntry holds one decoded KV interval.
// It mirrors C's flexdb_cache_entry but without SIMD / cacheline alignment.
type cacheEntry struct {
	fps    [sparseInterval]uint16 // 16-bit fingerprints per KV
	kvs    [sparseInterval]*KV    // decoded KV pointers
	count  int                    // number of valid KVs
	size   uint32                 // total logical size of all KVs
	frag   bool                   // fragmented; needs rewrite
	access uint16                 // clock replacement counter
	refcnt int32                  // active readers
	anchor *anchor                // back-pointer (nil when orphaned)

	// doubly-linked list within partition (for clock replacement)
	prev *cacheEntry
	next *cacheEntry
}

// cachePartition is one of 1024 independent shards.
type cachePartition struct {
	mu   sync.Mutex
	cap  uint64 // byte capacity
	size uint64 // current byte usage

	tick *cacheEntry // clock pointer
	ff   Storage
}

// dbCache is the full cache over all partitions.
type dbCache struct {
	partitions [cachePartitionCount]cachePartition
}

func newDBCache(ff Storage, capMB uint64) *dbCache {
	c := &dbCache{}
	perPart := capMB * (1 << 20) / cachePartitionCount
	for i := range c.partitions {
		c.partitions[i].cap = perPart
		c.partitions[i].ff = ff
	}
	return c
}

// partitionFor returns the partition responsible for the given anchor.
func (c *dbCache) partitionFor(a *anchor) *cachePartition {
	return &c.partitions[a.hash&cachePartitionMask]
}

// ---- cachePartition methods ----

// getEntry returns the cacheEntry for the given anchor, loading from flexfile if needed.
// Caller must call releaseEntry when done.
func (p *cachePartition) getEntry(a *anchor, loff uint64, itvbuf []byte) *cacheEntry {
	p.mu.Lock()
	e := a.cacheEntry
	if e != nil {
		e.refcnt++
		e.access = cacheEntryChance
		p.mu.Unlock()
		return e
	}
	// Allocate and mark as loading.
	e = &cacheEntry{anchor: a}
	e.refcnt = 1
	e.access = cacheEntryChance
	a.cacheEntry = e
	p.linkEntry(e)
	p.mu.Unlock()

	// Load from flexfile without holding the lock.
	p.loadEntry(e, a, loff, itvbuf)
	return e
}

// getEntryUnsorted returns the cacheEntry for anchor only when the interval can
// be decoded in sorted order (i.e. anchor.unsorted == 0 OR the caller has already
// decoded the unsorted tail). Returns nil when the unsorted quota is full.
func (p *cachePartition) getEntryUnsorted(a *anchor, loff uint64, itvbuf []byte) *cacheEntry {
	if int(a.unsorted) >= sparseInterval {
		return nil // quota exhausted
	}
	return p.getEntry(a, loff, itvbuf)
}

// releaseEntry decrements the reference count on e.
func (p *cachePartition) releaseEntry(e *cacheEntry) {
	p.mu.Lock()
	e.refcnt--
	p.mu.Unlock()
}

// linkEntry appends e to the partition's circular list (after tick or as sole element).
func (p *cachePartition) linkEntry(e *cacheEntry) {
	if p.tick == nil {
		e.prev = e
		e.next = e
		p.tick = e
		return
	}
	// Insert before tick.
	prev := p.tick.prev
	e.prev = prev
	e.next = p.tick
	prev.next = e
	p.tick.prev = e
}

// unlinkEntry removes e from the partition's circular list.
func (p *cachePartition) unlinkEntry(e *cacheEntry) {
	if e.next == e {
		// Sole element.
		p.tick = nil
		e.prev = nil
		e.next = nil
		return
	}
	if p.tick == e {
		p.tick = e.next
	}
	e.prev.next = e.next
	e.next.prev = e.prev
	e.prev = nil
	e.next = nil
}

// freeEntry evicts e from the partition (caller must hold mu).
func (p *cachePartition) freeEntry(e *cacheEntry) {
	p.unlinkEntry(e)
	p.size -= uint64(e.size)
	if e.anchor != nil {
		e.anchor.cacheEntry = nil
	}
	// KVs are GC'd automatically.
}

// findVictim finds a cache entry eligible for eviction (access==0, refcnt==0).
// Caller must hold mu. Returns nil if none found after a full sweep.
func (p *cachePartition) findVictim() *cacheEntry {
	if p.tick == nil {
		return nil
	}
	start := p.tick
	cur := start
	for {
		if cur.refcnt == 0 {
			if cur.access == 0 {
				return cur
			}
			cur.access--
		}
		cur = cur.next
		if cur == start {
			break
		}
	}
	return nil
}

// calibrate evicts entries until size <= cap.
func (p *cachePartition) calibrate(needed uint64) {
	for p.size+needed > p.cap {
		v := p.findVictim()
		if v == nil {
			break
		}
		p.freeEntry(v)
	}
}

// loadEntry reads the interval from flexfile into e. Called without mu held.
func (p *cachePartition) loadEntry(e *cacheEntry, a *anchor, loff uint64, itvbuf []byte) {
	if a.psize == 0 {
		return
	}
	psize := int(a.psize)
	if psize > len(itvbuf) {
		itvbuf = make([]byte, psize)
	}
	n, err := p.ff.Read(itvbuf[:psize], loff)
	if err != nil || n != psize {
		return
	}
	count := 0
	var totalSize uint32
	off := 0
	for off < psize && count < sparseInterval {
		kv, sz := decodeKV128(itvbuf[off:])
		if kv == nil || sz == 0 {
			break
		}
		e.kvs[count] = kv
		e.fps[count] = fp16(hash32(kv.Key))
		totalSize += uint32(kv128EncodedSize(len(kv.Key), len(kv.Value)))
		count++
		off += sz
	}
	e.count = count
	e.size = totalSize

	p.mu.Lock()
	p.size += uint64(totalSize)
	p.calibrate(0)
	p.mu.Unlock()
}

// ---- cache entry search ----

// findKeyEQ searches e for an exact match of key, returns the index or sparseInterval if not found.
func (e *cacheEntry) findKeyEQ(key []byte, fp uint16) int {
	// Scan 4 fingerprints at a time using uint64.
	// sparseInterval is 32, which is 4 * 8 bytes (4 * 4 uint16).
	fp4 := uint64(fp) | uint64(fp)<<16 | uint64(fp)<<32 | uint64(fp)<<48
	for i := 0; i < e.count; i += 4 {
		v := binary.LittleEndian.Uint64(unsafe.Slice((*byte)(unsafe.Pointer(&e.fps[i])), 8))
		// XOR with target fingerprints: matches will have 0000 in the corresponding 16-bit slot.
		x := v ^ fp4
		// Check if any slot is 0.
		// (x - 0x0001000100010001) & ^x & 0x8000800080008000 finds zero slots in 16-bit elements.
		if ((x - 0x0001000100010001) &^ x & 0x8000800080008000) != 0 {
			// At least one match in this block.
			for j := i; j < i+4 && j < e.count; j++ {
				if e.fps[j] == fp && compareKeys(key, e.kvs[j].Key) == 0 {
					return j
				}
			}
		}
	}
	return sparseInterval
}

// findKeyGE searches e for the insertion position of key (first entry with key >= key).
// Returns the index OR'd with (1<<31) when an exact match is found.
func (e *cacheEntry) findKeyGE(key []byte, fp uint16) int {
	// Check fingerprints first using the same bitwise trick to find possible exact matches.
	fp4 := uint64(fp) | uint64(fp)<<16 | uint64(fp)<<32 | uint64(fp)<<48
	for i := 0; i < e.count; i += 4 {
		v := binary.LittleEndian.Uint64(unsafe.Slice((*byte)(unsafe.Pointer(&e.fps[i])), 8))
		x := v ^ fp4
		if ((x - 0x0001000100010001) &^ x & 0x8000800080008000) != 0 {
			for j := i; j < i+4 && j < e.count; j++ {
				if e.fps[j] == fp && compareKeys(key, e.kvs[j].Key) == 0 {
					return j | (1 << 31)
				}
			}
		}
	}

	// Binary search for position if no exact match.
	lo, hi := 0, e.count
	for lo < hi {
		mid := (lo + hi) >> 1
		cmp := compareKeys(key, e.kvs[mid].Key)
		if cmp > 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// ---- cache entry mutation ----

// insertKV inserts kv at position i, shifting others right. Updates size.
func (e *cacheEntry) insertKV(p *cachePartition, kv *KV, i int) {
	sz := uint32(kv128EncodedSize(len(kv.Key), len(kv.Value)))
	copy(e.kvs[i+1:e.count+1], e.kvs[i:e.count])
	copy(e.fps[i+1:e.count+1], e.fps[i:e.count])
	e.kvs[i] = kv
	e.fps[i] = fp16(hash32(kv.Key))
	e.count++
	e.size += sz
	p.mu.Lock()
	p.size += uint64(sz)
	p.mu.Unlock()
}

// replaceKV replaces the KV at position i with kv. Updates size delta.
func (e *cacheEntry) replaceKV(p *cachePartition, kv *KV, i int) {
	oldSz := uint32(kv128EncodedSize(len(e.kvs[i].Key), len(e.kvs[i].Value)))
	newSz := uint32(kv128EncodedSize(len(kv.Key), len(kv.Value)))
	e.kvs[i] = kv
	// Don't update fps since key may be the same byte content.
	p.mu.Lock()
	p.size = p.size - uint64(oldSz) + uint64(newSz)
	p.mu.Unlock()
	e.size = e.size - oldSz + newSz
}

// deleteKV deletes the KV at position i, shifting others left.
func (e *cacheEntry) deleteKV(p *cachePartition, i int) {
	sz := uint32(kv128EncodedSize(len(e.kvs[i].Key), len(e.kvs[i].Value)))
	copy(e.kvs[i:e.count-1], e.kvs[i+1:e.count])
	copy(e.fps[i:e.count-1], e.fps[i+1:e.count])
	e.kvs[e.count-1] = nil
	e.count--
	e.size -= sz
	p.mu.Lock()
	p.size -= uint64(sz)
	p.mu.Unlock()
}

// clearFrag marks the entry as no longer fragmented.
func (e *cacheEntry) clearFrag() { e.frag = false }

// stats returns the total bytes in use and total byte capacity across all partitions.
func (c *dbCache) stats() (used, cap uint64) {
	for i := range c.partitions {
		p := &c.partitions[i]
		p.mu.Lock()
		used += p.size
		cap += p.cap
		p.mu.Unlock()
	}
	return
}
