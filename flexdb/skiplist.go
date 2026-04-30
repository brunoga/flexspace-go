package flexdb

import (
	"bytes"
	"sync/atomic"
)

// maxSkipLevel is the maximum height of the skip list.
// With p=0.25, log_4(n) expected levels: 20 covers 4^20 ≈ 1 trillion entries.
const maxSkipLevel = 20

// skipInlineLevel is the number of next pointers stored directly in skipNode.
// Levels 0..skipInlineLevel-1 are inline (no extra allocation or indirection).
// Levels skipInlineLevel+ use tower, which is only allocated for ~6% of nodes
// with p=0.5, or ~0.4% with p=0.25.
const skipInlineLevel = 4

// skipNode layout:
//
//	key   []byte
//	value atomic.Pointer[[]byte]
//	next  [skipInlineLevel]atomic.Pointer[skipNode]
//	tower []atomic.Pointer[skipNode]
//
// All pointer fields are atomic so readers and writers can proceed concurrently.
type skipNode struct {
	key   []byte
	value atomic.Pointer[[]byte]
	next  [skipInlineLevel]atomic.Pointer[skipNode]
	tower []atomic.Pointer[skipNode]
}

type skipList struct {
	head  skipNode // sentinel; tower pre-allocated at maxSkipLevel
	level atomic.Int32
	rng   atomic.Uint64 // xorshift64 PRNG
}

func newSkipList() *skipList {
	sl := &skipList{}
	sl.level.Store(1)
	sl.rng.Store(42)
	if maxSkipLevel > skipInlineLevel {
		sl.head.tower = make([]atomic.Pointer[skipNode], maxSkipLevel-skipInlineLevel)
	}
	return sl
}

// randomLevel returns a level in [1, maxSkipLevel] with p = 0.25.
// Uses a CAS-based PRNG so concurrent callers each get a distinct state.
func (sl *skipList) randomLevel() int {
	for {
		old := sl.rng.Load()
		nxt := old
		nxt ^= nxt << 13
		nxt ^= nxt >> 7
		nxt ^= nxt << 17
		if sl.rng.CompareAndSwap(old, nxt) {
			r := nxt
			lvl := 1
			for lvl < maxSkipLevel && r&3 == 0 {
				r >>= 2
				lvl++
			}
			return lvl
		}
	}
}

// nodeNext returns the level-i next pointer of n using an atomic load.
func nodeNext(n *skipNode, i int) *skipNode {
	if i < skipInlineLevel {
		return n.next[i].Load()
	}
	j := i - skipInlineLevel
	if j < len(n.tower) {
		return n.tower[j].Load()
	}
	return nil
}

// nodeStore atomically stores v as the level-i next pointer of n.
func nodeStore(n *skipNode, i int, v *skipNode) {
	if i < skipInlineLevel {
		n.next[i].Store(v)
	} else {
		n.tower[i-skipInlineLevel].Store(v)
	}
}

// nodeCAS atomically replaces n's level-i next pointer using CAS.
// A node at level i is always in the level-i list and therefore has tower
// capacity for level i, so the tower bounds check is a safety fallback only.
func nodeCAS(n *skipNode, i int, old, nw *skipNode) bool {
	if i < skipInlineLevel {
		return n.next[i].CompareAndSwap(old, nw)
	}
	j := i - skipInlineLevel
	if j < len(n.tower) {
		return n.tower[j].CompareAndSwap(old, nw)
	}
	return false
}

// find locates the predecessor and successor of key at every level.
// Returns the node at level 0 if key is already present, else nil.
// preds[i] is set to the rightmost node at level i with key < target;
// succs[i] is preds[i]'s level-i successor (first node with key >= target).
func (sl *skipList) find(key []byte, preds *[maxSkipLevel]*skipNode, succs *[maxSkipLevel]*skipNode) *skipNode {
	cur := &sl.head
	level := int(sl.level.Load())
	for i := level - 1; i >= 0; i-- {
		for {
			nxt := nodeNext(cur, i)
			if nxt == nil || bytes.Compare(nxt.key, key) >= 0 {
				preds[i] = cur
				succs[i] = nxt
				break
			}
			cur = nxt
		}
	}
	if succs[0] != nil && bytes.Equal(succs[0].key, key) {
		return succs[0]
	}
	return nil
}

// Set inserts or updates key with value.
//
// Concurrency model — fully lock-free:
//   - Multiple writers can call Set concurrently without a mutex.
//   - A new node is published by CAS-ing the level-0 link first so Get() always
//     finds it at level 0 even before higher-level links are established.
//   - At each level i: n.next[i] is stored before the predecessor's CAS so any
//     reader that observes n at level i also sees a valid n.next[i].
//   - preds[] is pre-filled with &sl.head so levels above the current list height
//     (which find() does not traverse) have a valid, fully-towered CAS target.
func (sl *skipList) Set(key, value []byte) {
	for {
		var preds [maxSkipLevel]*skipNode
		for i := range preds {
			preds[i] = &sl.head
		}
		var succs [maxSkipLevel]*skipNode
		found := sl.find(key, &preds, &succs)

		if found != nil {
			v := &value
			found.value.Store(v)
			return
		}

		lvl := sl.randomLevel()
		// Extend the list height if needed.
		for {
			oldLvl := sl.level.Load()
			if int32(lvl) <= oldLvl || sl.level.CompareAndSwap(oldLvl, int32(lvl)) {
				break
			}
		}

		n := &skipNode{key: key}
		v := &value
		n.value.Store(v)
		if lvl > skipInlineLevel {
			n.tower = make([]atomic.Pointer[skipNode], lvl-skipInlineLevel)
		}

		// Publish at level 0 first. If the CAS fails, someone else modified
		// preds[0] between find() and now — restart the whole operation.
		nodeStore(n, 0, succs[0])
		if !nodeCAS(preds[0], 0, succs[0], n) {
			continue
		}

		// Level 0 succeeded: n is now visible to Get() and iterators.
		// Link higher levels one at a time. A CAS failure here only means
		// another insert raced at that level; refind and retry that level.
		for i := 1; i < lvl; i++ {
			for {
				nodeStore(n, i, succs[i])
				if nodeCAS(preds[i], i, succs[i], n) {
					break
				}
				sl.find(key, &preds, &succs)
			}
		}
		return
	}
}

// Get returns (value, true) if key is present, else (nil, false).
// Fully lock-free: safe to call concurrently with Set and other Gets.
func (sl *skipList) Get(key []byte) ([]byte, bool) {
	cur := &sl.head
	level := int(sl.level.Load())
	for i := level - 1; i >= 0; i-- {
		for {
			nxt := nodeNext(cur, i)
			if nxt == nil || bytes.Compare(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
	}
	n := nodeNext(cur, 0)
	if n != nil && bytes.Equal(n.key, key) {
		v := n.value.Load()
		if v != nil {
			return *v, true
		}
		return nil, true // tombstone (nil value pointer)
	}
	return nil, false
}

// Seek returns an iterator positioned at the first key >= key.
// Lock-free: safe to call concurrently with Set.
func (sl *skipList) Seek(key []byte) *skipIter {
	cur := &sl.head
	level := int(sl.level.Load())
	for i := level - 1; i >= 0; i-- {
		for {
			nxt := nodeNext(cur, i)
			if nxt == nil || bytes.Compare(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
	}
	return &skipIter{cur: nodeNext(cur, 0)}
}

// First returns an iterator at the first (smallest) key.
func (sl *skipList) First() *skipIter {
	return &skipIter{cur: nodeNext(&sl.head, 0)}
}

// skipIter is a forward iterator over a skipList.
type skipIter struct {
	cur *skipNode
}

func (it *skipIter) Valid() bool { return it.cur != nil }
func (it *skipIter) Key() []byte { return it.cur.key }
func (it *skipIter) Value() []byte {
	v := it.cur.value.Load()
	if v != nil {
		return *v
	}
	return nil
}
func (it *skipIter) Next() { it.cur = nodeNext(it.cur, 0) }
