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
type skipNode struct {
	key   []byte
	value atomic.Pointer[[]byte]
	next  [skipInlineLevel]atomic.Pointer[skipNode]
	tower []atomic.Pointer[skipNode]
}

type skipList struct {
	head  skipNode // sentinel; tower pre-allocated
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
func (sl *skipList) randomLevel() int {
	for {
		old := sl.rng.Load()
		new := old
		new ^= new << 13
		new ^= new >> 7
		new ^= new << 17
		if sl.rng.CompareAndSwap(old, new) {
			r := new
			lvl := 1
			for lvl < maxSkipLevel && r&3 == 0 {
				r >>= 2
				lvl++
			}
			return lvl
		}
	}
}

// nodeNext returns the level-i next pointer of n.
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

// nodeSetNext sets the level-i next pointer of n.
func nodeSetNext(n *skipNode, i int, v *skipNode) {
	if i < skipInlineLevel {
		n.next[i].Store(v)
	} else {
		n.tower[i-skipInlineLevel].Store(v)
	}
}

// nodeCompareAndSwapNext updates the level-i next pointer of n using CAS.
func nodeCompareAndSwapNext(n *skipNode, i int, old, new *skipNode) bool {
	if i < skipInlineLevel {
		return n.next[i].CompareAndSwap(old, new)
	}
	return n.tower[i-skipInlineLevel].CompareAndSwap(old, new)
}

// Set inserts or updates key with value.
// Safe for concurrent use: multiple goroutines may call Set simultaneously.
func (sl *skipList) Set(key, value []byte) {
	for {
		// Default preds to head so levels not visited by find (when the new
		// node's height exceeds the current list height) have a valid CAS target.
		var preds [maxSkipLevel]*skipNode
		for i := range preds {
			preds[i] = &sl.head
		}
		var succs [maxSkipLevel]*skipNode
		found := sl.find(key, &preds, &succs)

		if found != nil {
			// Key exists, update value atomically.
			v := &value
			found.value.Store(v)
			return
		}

		// Key doesn't exist, insert new node.
		lvl := sl.randomLevel()
		for {
			oldLvl := sl.level.Load()
			if int32(lvl) <= oldLvl || sl.level.CompareAndSwap(oldLvl, int32(lvl)) {
				break
			}
		}

		newNode := &skipNode{key: key}
		v := &value
		newNode.value.Store(v)
		if lvl > skipInlineLevel {
			newNode.tower = make([]atomic.Pointer[skipNode], lvl-skipInlineLevel)
		}

		// Link level 0 first.
		nodeSetNext(newNode, 0, succs[0])
		if !nodeCompareAndSwapNext(preds[0], 0, succs[0], newNode) {
			continue // concurrent insert at level 0, retry from scratch
		}

		// Level 0 success! Now link higher levels.
		for i := 1; i < lvl; i++ {
			for {
				nodeSetNext(newNode, i, succs[i])
				if nodeCompareAndSwapNext(preds[i], i, succs[i], newNode) {
					break // success for this level
				}
				// Concurrent change at this level. Re-find to get updated pred/succ.
				sl.find(key, &preds, &succs)
			}
		}
		return
	}
}

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

// Get returns (value, true) if key is present, else (nil, false).
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
		return nil, true // logically deleted if value is nil (not possible with current Set but for safety)
	}
	return nil, false
}

// Seek returns an iterator at the first key >= key.
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
// Relies on Go GC to reclaim the detached nodes.
func (sl *skipList) Clear() {
	for i := range sl.head.next {
		sl.head.next[i].Store(nil)
	}
	for i := range sl.head.tower {
		sl.head.tower[i].Store(nil)
	}
	sl.level.Store(1)
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
