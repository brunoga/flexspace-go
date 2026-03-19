package flexdb

import "sync"

// maxSkipLevel is the maximum height of the skip list.
// With p=0.25, log_4(n) expected levels: 20 covers 4^20 ≈ 1 trillion entries.
const maxSkipLevel = 20

// skipInlineLevel is the number of next pointers stored directly in skipNode.
// Levels 0..skipInlineLevel-1 are inline (no extra allocation or indirection).
// Levels skipInlineLevel+ use tower, which is only allocated for ~6% of nodes
// with p=0.5, or ~0.4% with p=0.25.
const skipInlineLevel = 4

// skipNode layout (104 bytes → sizeclass 112):
//
//	offset  0-23: key  []byte  — cache line 0
//	offset 24-47: value []byte — cache line 0
//	offset 48-55: next[0]      — cache line 0   ← hot level-0 traversal
//	offset 56-63: next[1]      — cache line 0   ← hot level-1 traversal
//	offset 64-71: next[2]      — cache line 1
//	offset 72-79: next[3]      — cache line 1
//	offset 80-103: tower       — cache line 1   (nil for ~94% of nodes)
//
// Compared to the old [24]*skipNode (240 bytes = 4 cache lines), this design
// is ~2× smaller on average: better L3 utilisation for large skip lists.
type skipNode struct {
	key   []byte
	value []byte
	next  [skipInlineLevel]*skipNode // levels 0..skipInlineLevel-1, inline
	tower []*skipNode                // levels skipInlineLevel+, rare
}

// skipNodePool recycles skipNode objects to reduce allocations on the Put path.
// Get returns nil on first use; Set handles that by allocating fresh nodes.
var skipNodePool = sync.Pool{}

type skipList struct {
	head  skipNode // sentinel; tower pre-allocated
	level int
	rng   uint64 // xorshift64 PRNG — no interface dispatch, no heap allocation
}

func newSkipList() *skipList {
	sl := &skipList{level: 1, rng: 42}
	if maxSkipLevel > skipInlineLevel {
		sl.head.tower = make([]*skipNode, maxSkipLevel-skipInlineLevel)
	}
	return sl
}

// randomLevel returns a level in [1, maxSkipLevel] with p = 0.25.
// p=0.25 keeps 75% of nodes at level 1 (no tower), reducing allocation pressure.
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

// nodeNext returns the level-i next pointer of n.
// Inlined helper; avoids a branch in the hot traversal loops
// by splitting inline vs tower paths at the call sites below.
func nodeNext(n *skipNode, i int) *skipNode {
	if i < skipInlineLevel {
		return n.next[i]
	}
	j := i - skipInlineLevel
	if j < len(n.tower) {
		return n.tower[j]
	}
	return nil
}

// nodeSetNext sets the level-i next pointer of n.
func nodeSetNext(n *skipNode, i int, v *skipNode) {
	if i < skipInlineLevel {
		n.next[i] = v
	} else {
		n.tower[i-skipInlineLevel] = v
	}
}

// Set inserts or updates key with value.
func (sl *skipList) Set(key, value []byte) {
	var update [maxSkipLevel]*skipNode
	cur := &sl.head

	// Traverse upper levels (tower path).  These levels have very few nodes
	// so the tower backing array is nearly always cache-hot.
	for i := sl.level - 1; i >= skipInlineLevel; i-- {
		j := i - skipInlineLevel
		for {
			var nxt *skipNode
			if j < len(cur.tower) {
				nxt = cur.tower[j]
			}
			if nxt == nil || compareKeys(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
		update[i] = cur
	}

	// Traverse inline levels (direct array access, no function call or branch).
	top := sl.level - 1
	if top >= skipInlineLevel {
		top = skipInlineLevel - 1
	}
	for i := top; i >= 0; i-- {
		for {
			nxt := cur.next[i]
			if nxt == nil || compareKeys(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
		update[i] = cur
	}

	// Update in place if key already exists.
	if nxt := update[0].next[0]; nxt != nil && compareKeys(nxt.key, key) == 0 {
		nxt.value = value
		return
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = &sl.head
		}
		sl.level = lvl
	}

	var n *skipNode
	if pooled, ok := skipNodePool.Get().(*skipNode); ok && pooled != nil {
		n = pooled
		n.key = key
		n.value = value
	} else {
		n = &skipNode{key: key, value: value}
	}
	if lvl > skipInlineLevel {
		need := lvl - skipInlineLevel
		if cap(n.tower) >= need {
			n.tower = n.tower[:need]
		} else {
			n.tower = make([]*skipNode, need)
		}
	} else {
		n.tower = n.tower[:0]
	}
	for i := range lvl {
		nodeSetNext(n, i, nodeNext(update[i], i))
		nodeSetNext(update[i], i, n)
	}
}

// Get returns (value, true) if key is present, else (nil, false).
func (sl *skipList) Get(key []byte) ([]byte, bool) {
	cur := &sl.head
	for i := sl.level - 1; i >= skipInlineLevel; i-- {
		j := i - skipInlineLevel
		for {
			var nxt *skipNode
			if j < len(cur.tower) {
				nxt = cur.tower[j]
			}
			if nxt == nil || compareKeys(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
	}
	top := sl.level - 1
	if top >= skipInlineLevel {
		top = skipInlineLevel - 1
	}
	for i := top; i >= 0; i-- {
		for {
			nxt := cur.next[i]
			if nxt == nil || compareKeys(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
	}
	n := cur.next[0]
	if n != nil && compareKeys(n.key, key) == 0 {
		return n.value, true
	}
	return nil, false
}

// Seek returns an iterator at the first key >= key.
func (sl *skipList) Seek(key []byte) *skipIter {
	cur := &sl.head
	for i := sl.level - 1; i >= skipInlineLevel; i-- {
		j := i - skipInlineLevel
		for {
			var nxt *skipNode
			if j < len(cur.tower) {
				nxt = cur.tower[j]
			}
			if nxt == nil || compareKeys(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
	}
	top := sl.level - 1
	if top >= skipInlineLevel {
		top = skipInlineLevel - 1
	}
	for i := top; i >= 0; i-- {
		for {
			nxt := cur.next[i]
			if nxt == nil || compareKeys(nxt.key, key) >= 0 {
				break
			}
			cur = nxt
		}
	}
	return &skipIter{cur: cur.next[0]}
}

// First returns an iterator at the first (smallest) key.
func (sl *skipList) First() *skipIter {
	return &skipIter{cur: sl.head.next[0]}
}

// Clear removes all entries without releasing the head's tower allocation.
// Traverses the level-0 chain and returns all nodes to skipNodePool for reuse.
func (sl *skipList) Clear() {
	for n := sl.head.next[0]; n != nil; {
		next := n.next[0]
		n.key = nil
		n.value = nil
		for i := range n.next {
			n.next[i] = nil
		}
		// Keep the tower slice alive (capacity reused in Set).
		for i := range n.tower {
			n.tower[i] = nil
		}
		skipNodePool.Put(n)
		n = next
	}
	for i := range sl.head.next {
		sl.head.next[i] = nil
	}
	for i := range sl.head.tower {
		sl.head.tower[i] = nil
	}
	sl.level = 1
}

// skipIter is a forward iterator over a skipList.
type skipIter struct {
	cur *skipNode
}

func (it *skipIter) Valid() bool   { return it.cur != nil }
func (it *skipIter) Key() []byte   { return it.cur.key }
func (it *skipIter) Value() []byte { return it.cur.value }
func (it *skipIter) Next()         { it.cur = it.cur.next[0] }
