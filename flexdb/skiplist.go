package flexdb

import (
	"bytes"
	"sync"
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
// All pointer fields are atomic so readers can traverse without holding mu.
type skipNode struct {
	key   []byte
	value atomic.Pointer[[]byte]
	next  [skipInlineLevel]atomic.Pointer[skipNode]
	tower []atomic.Pointer[skipNode]
}

type skipList struct {
	head  skipNode   // sentinel; tower pre-allocated at maxSkipLevel
	mu    sync.Mutex // serializes all writes; readers are lock-free
	level atomic.Int32
	rng   uint64 // xorshift64 PRNG — only accessed under mu
}

func newSkipList() *skipList {
	sl := &skipList{rng: 42}
	sl.level.Store(1)
	if maxSkipLevel > skipInlineLevel {
		sl.head.tower = make([]atomic.Pointer[skipNode], maxSkipLevel-skipInlineLevel)
	}
	return sl
}

// randomLevel returns a level in [1, maxSkipLevel] with p = 0.25.
// Must be called with mu held.
func (sl *skipList) randomLevel() int {
	sl.rng ^= sl.rng << 13
	sl.rng ^= sl.rng >> 7
	sl.rng ^= sl.rng << 17
	r := sl.rng
	lvl := 1
	for lvl < maxSkipLevel && r&3 == 0 {
		r >>= 2
		lvl++
	}
	return lvl
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

// nodeStore sets the level-i next pointer of n using an atomic store.
func nodeStore(n *skipNode, i int, v *skipNode) {
	if i < skipInlineLevel {
		n.next[i].Store(v)
	} else {
		n.tower[i-skipInlineLevel].Store(v)
	}
}

// Set inserts or updates key with value.
// Writers are serialized by mu; concurrent Gets and Seeks are lock-free.
//
// Insertion publishes the new node at level 0 first so that readers always
// observe a fully-linked level-0 chain. Higher-level links follow immediately
// (still under mu, so no other writer can interfere), and each level's forward
// pointer is stored into the new node before the node is spliced into the
// predecessor, guaranteeing that a reader that sees the new node at level i
// also sees a valid forward pointer at level i.
func (sl *skipList) Set(key, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	var preds [maxSkipLevel]*skipNode
	var succs [maxSkipLevel]*skipNode

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

	// Update value in place if key already exists.
	if succs[0] != nil && bytes.Equal(succs[0].key, key) {
		v := &value
		succs[0].value.Store(v)
		return
	}

	lvl := sl.randomLevel()
	if lvl > level {
		for i := level; i < lvl; i++ {
			preds[i] = &sl.head
			succs[i] = nil
		}
		sl.level.Store(int32(lvl))
	}

	n := &skipNode{key: key}
	v := &value
	n.value.Store(v)
	if lvl > skipInlineLevel {
		n.tower = make([]atomic.Pointer[skipNode], lvl-skipInlineLevel)
	}

	// Publish at level 0 first, then link higher levels in order.
	// At each level: initialize n's forward pointer before splicing n in,
	// so readers that observe n at level i always see a valid n.next[i].
	nodeStore(n, 0, succs[0])
	nodeStore(preds[0], 0, n)
	for i := 1; i < lvl; i++ {
		nodeStore(n, i, succs[i])
		nodeStore(preds[i], i, n)
	}
}

// Get returns (value, true) if key is present, else (nil, false).
// Lock-free: safe to call concurrently with Set and other Gets.
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

// Clear removes all entries by severing the head's pointers.
// Callers must ensure no readers are active (memtable.reset() spins on readers
// reaching zero before calling this).
func (sl *skipList) Clear() {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	for i := range sl.head.next {
		sl.head.next[i].Store(nil)
	}
	for i := range sl.head.tower {
		sl.head.tower[i].Store(nil)
	}
	sl.level.Store(1)
	sl.rng = 42
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
