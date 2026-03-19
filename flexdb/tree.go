package flexdb

// Sparse anchor B+tree — in-memory index over the flexfile's key intervals.
//
// This mirrors the C flexdb_tree / flexdb_tree_node structures.
// Leaf nodes hold up to leafCap anchors; internal nodes hold up to internalCap pivots.
// Each internal child carries a shift (int64) that must be added to stored loff
// values in the subtree beneath it to recover the true logical file offset.
// This lets us keep anchor.loff as uint32 without overflow.

const (
	leafCap     = 122 // FLEXDB_TREE_LEAF_CAP
	internalCap = 40  // FLEXDB_TREE_INTERNAL_CAP
)

// anchor is one entry in a leaf node. It corresponds to one sorted key interval
// stored sequentially in the flexfile starting at (loff + accumulated shift).
type anchor struct {
	key      []byte
	loff     uint32 // relative to the accumulated shift through parent chain
	psize    uint32 // physical byte size of the interval in the file
	unsorted uint8  // number of unsorted appends on top of the sorted interval
	hash     uint32 // FNV-1a hash of key, used for cache-partition selection

	cacheEntry *cacheEntry // nil when not cached
}

// treeNode is a B+tree node — leaf or internal.
type treeNode struct {
	isLeaf   bool
	count    uint32 // number of anchors (leaf) or pivots (internal)
	parentID uint32 // index of this node in parent.children
	tree     *dbTree
	parent   *treeNode

	// leaf fields
	anchors [leafCap]*anchor
	prev    *treeNode
	next    *treeNode

	// internal fields
	pivots   [internalCap][]byte
	children [internalCap + 1]struct {
		node  *treeNode
		shift int64
	}
}

// dbTree is the sparse anchor index.
type dbTree struct {
	root     *treeNode
	leafHead *treeNode
}

func newDBTree() *dbTree {
	t := &dbTree{}
	root := &treeNode{isLeaf: true, tree: t}
	t.root = root
	t.leafHead = root
	// Insert the sentinel anchor (empty key, loff 0)
	a := &anchor{key: []byte{}, loff: 0}
	a.hash = hash32(a.key)
	root.anchors[0] = a
	root.count = 1
	return t
}

// anchorCount returns the total number of leaf anchors across all leaf nodes.
func (t *dbTree) anchorCount() int {
	count := 0
	for n := t.leafHead; n != nil; n = n.next {
		count += int(n.count)
	}
	return count
}

// nodeHandler is the result of a tree search: the leaf node and position found.
type nodeHandler struct {
	node  *treeNode
	shift int64  // accumulated shift from root to leaf
	idx   uint32 // position within leaf (≤ node.count-1)
}

// findAnchorPos locates the rightmost anchor whose key ≤ kref in the tree.
func (t *dbTree) findAnchorPos(key []byte) nodeHandler {
	var shift int64
	node := t.root
	for !node.isLeaf {
		idx := findPosInInternal(node, key)
		shift += node.children[idx].shift
		node = node.children[idx].node
	}
	return nodeHandler{node: node, shift: shift, idx: findPosInLeafLE(node, key)}
}

// findPosInInternal returns the child index for key in an internal node.
// Implements upper_bound: returns the first child whose pivot > key.
func findPosInInternal(node *treeNode, key []byte) uint32 {
	lo, hi := uint32(0), node.count
	for lo < hi {
		mid := (lo + hi) >> 1
		if compareKeys(key, node.pivots[mid]) >= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// findPosInLeafLE returns the index of the rightmost anchor whose key ≤ key.
func findPosInLeafLE(node *treeNode, key []byte) uint32 {
	lo, hi := uint32(0), node.count
	for lo+1 < hi {
		mid := (lo + hi) >> 1
		cmp := compareKeys(key, node.anchors[mid].key)
		if cmp > 0 {
			lo = mid
		} else if cmp < 0 {
			hi = mid
		} else {
			return mid
		}
	}
	return lo
}

// insertAnchor inserts a new anchor after the position described by nh.
// Returns the newly created anchor.
func (t *dbTree) insertAnchor(nh *nodeHandler, key []byte, loff uint64, psize uint32) *anchor {
	node := nh.node
	a := &anchor{
		key:   key,
		loff:  uint32(loff - uint64(nh.shift)),
		psize: psize,
	}
	a.hash = hash32(key)

	target := nh.idx
	// Insert at target+1 (after the found position, which is the anchor for the
	// interval that this new anchor splits off from).
	insertAt := min(target+1, node.count)
	// Shift anchors right.
	copy(node.anchors[insertAt+1:node.count+1], node.anchors[insertAt:node.count])
	node.anchors[insertAt] = a
	node.count++

	if t.nodeIsFull(node) {
		t.splitLeafNode(node)
	}
	return a
}

func (t *dbTree) nodeIsFull(node *treeNode) bool {
	if node.isLeaf {
		return node.count >= leafCap-1
	}
	return node.count >= internalCap-1
}

func (t *dbTree) nodeIsEmpty(node *treeNode) bool {
	return node.count == 0
}

// rebaseLeaf normalises stored loff values so they don't overflow uint32.
// Mirrors C's flexdb_tree_node_rebase.
func rebaseLeaf(node *treeNode) {
	if node.count == 0 || node.parent == nil {
		return
	}
	last := node.anchors[node.count-1].loff
	if last < (^uint32(0))>>2 {
		return
	}
	base := uint64(node.anchors[0].loff)
	if base == 0 {
		return
	}
	node.parent.children[node.parentID].shift += int64(base)
	for i := uint32(0); i < node.count; i++ {
		node.anchors[i].loff -= uint32(base)
	}
}

// shiftUpPropagate adds shift to all anchors after idx in the leaf and
// propagates to parent chains.
func shiftUpPropagate(nh *nodeHandler, shift int64) {
	node := nh.node
	target := nh.idx
	for i := target + 1; i < node.count; i++ {
		node.anchors[i].loff = uint32(int64(node.anchors[i].loff) + shift)
	}
	cur := node
	for cur.parent != nil {
		pIdx := cur.parentID
		cur = cur.parent
		for i := pIdx; i < cur.count; i++ {
			cur.children[i+1].shift += shift
		}
	}
}

// accumulatedShift recomputes the shift for nh by walking up to root.
func accumulatedShift(nh *nodeHandler) {
	node := nh.node
	var shift int64
	cur := node
	for cur.parent != nil {
		pIdx := cur.parentID
		cur = cur.parent
		shift += cur.children[pIdx].shift
	}
	nh.shift = shift
}

// linkTwoLeaves links node2 after node1 in the leaf doubly-linked list.
func linkTwoLeaves(node1, node2 *treeNode) {
	node2.prev = node1
	node2.next = node1.next
	node1.next = node2
	if node2.next != nil {
		node2.next.prev = node2
	}
}

// splitLeafNode splits a full leaf node into two.
func (t *dbTree) splitLeafNode(node *treeNode) {
	node2 := &treeNode{isLeaf: true, tree: t, parent: node.parent}
	linkTwoLeaves(node, node2)

	count := (node.count + 1) / 2
	node2.count = node.count - count
	copy(node2.anchors[:node2.count], node.anchors[count:node.count])
	node.count = count

	parent := node.parent
	if parent == nil {
		parent = &treeNode{tree: t}
		node.parent = parent
		node2.parent = parent
		t.root = parent
	}

	if parent.count == 0 {
		parent.children[0].node = node
		parent.children[0].shift = 0
		parent.children[1].node = node2
		parent.children[1].shift = 0
		// Duplicate the first key of node2 as the pivot.
		parent.pivots[0] = dupKey(node2.anchors[0].key)
		node.parentID = 0
		node2.parentID = 1
		parent.count = 1
	} else {
		pIdx := node.parentID
		origShift := parent.children[pIdx].shift
		// Shift pivots right.
		copy(parent.pivots[pIdx+1:parent.count+1], parent.pivots[pIdx:parent.count])
		// Shift children right.
		copy(parent.children[pIdx+2:parent.count+2], parent.children[pIdx+1:parent.count+1])
		parent.children[pIdx+1].node = node2
		parent.children[pIdx+1].shift = origShift
		parent.pivots[pIdx] = dupKey(node2.anchors[0].key)
		node2.parentID = node.parentID
		parent.count++
		for i := pIdx + 1; i < parent.count+1; i++ {
			parent.children[i].node.parentID++
		}
	}

	if parent.count > 1 {
		rebaseLeaf(node)
		rebaseLeaf(node2)
	}

	if t.nodeIsFull(parent) {
		t.splitInternalNode(parent)
	}
}

// splitInternalNode splits a full internal node.
func (t *dbTree) splitInternalNode(node *treeNode) {
	node2 := &treeNode{tree: t, parent: node.parent}

	count := (node.count + 1) / 2
	newBase := node.pivots[count] // the pivot that goes up

	node2.count = node.count - count - 1
	copy(node2.pivots[:node2.count], node.pivots[count+1:node.count])
	copy(node2.children[:node2.count+1], node.children[count+1:node.count+1])
	node.count = count

	parent := node.parent
	if parent == nil {
		parent = &treeNode{tree: t}
		node.parent = parent
		node2.parent = parent
		t.root = parent
	}

	if parent.count == 0 {
		parent.children[0].node = node
		parent.children[0].shift = 0
		parent.children[1].node = node2
		parent.children[1].shift = 0
		parent.pivots[0] = newBase
		node.parentID = 0
		node2.parentID = 1
		parent.count = 1
	} else {
		pIdx := node.parentID
		origShift := parent.children[pIdx].shift
		copy(parent.pivots[pIdx+1:parent.count+1], parent.pivots[pIdx:parent.count])
		copy(parent.children[pIdx+2:parent.count+2], parent.children[pIdx+1:parent.count+1])
		parent.children[pIdx+1].node = node2
		parent.children[pIdx+1].shift = origShift
		parent.pivots[pIdx] = newBase
		node2.parentID = node.parentID
		parent.count++
		for i := pIdx + 1; i < parent.count+1; i++ {
			parent.children[i].node.parentID++
		}
	}

	// Fix parent pointers in node2's children.
	for i := uint32(0); i < node2.count+1; i++ {
		node2.children[i].node.parentID = i
		node2.children[i].node.parent = node2
	}

	if t.nodeIsFull(parent) {
		t.splitInternalNode(parent)
	}
}

// updateSmallestKey walks up to the first non-leftmost ancestor and updates its pivot.
func updateSmallestKey(since *treeNode, key []byte) {
	pIdx := since.parentID
	node := since.parent
	for node != nil {
		if pIdx != 0 {
			break
		}
		pIdx = node.parentID
		node = node.parent
	}
	if node != nil {
		node.pivots[pIdx-1] = dupKey(key)
	}
}

// findSmallestKey returns the leftmost key of the subtree rooted at node.
func findSmallestKey(node *treeNode) []byte {
	for !node.isLeaf {
		node = node.children[0].node
	}
	return node.anchors[0].key
}

// removeAnchorAt removes the anchor at nh.idx from the leaf.
// If the leaf becomes empty, it recycles the node.
func (t *dbTree) removeAnchorAt(nh *nodeHandler) {
	node := nh.node
	idx := nh.idx

	// Detach cache entry.
	a := node.anchors[idx]
	if a.cacheEntry != nil {
		a.cacheEntry.anchor = nil
		a.cacheEntry = nil
	}

	// Shift remaining anchors left.
	copy(node.anchors[idx:node.count-1], node.anchors[idx+1:node.count])
	node.anchors[node.count-1] = nil
	node.count--

	if node.count == 0 {
		t.recycleNode(node)
		return
	}

	// Update parent pivot if we removed the first anchor.
	if idx == 0 {
		updateSmallestKey(node, node.anchors[0].key)
	}
}

// recycleLinkedList unlinks node from the leaf doubly-linked list.
func (t *dbTree) recycleLinkedList(node *treeNode) {
	prev := node.prev
	next := node.next
	if prev != nil {
		prev.next = next
	} else {
		t.leafHead = next
	}
	if next != nil {
		next.prev = prev
	}
}

// recycleNode removes an empty node from the tree, collapsing parent if needed.
func (t *dbTree) recycleNode(node *treeNode) {
	if node.isLeaf {
		t.recycleLinkedList(node)
	}

	parent := node.parent
	if parent == nil {
		return // root; shouldn't happen
	}
	pIdx := node.parentID

	if parent.count == 1 {
		// Parent has only one pivot → it also becomes empty after we remove node.
		sIdx := uint32(0)
		if pIdx == 0 {
			sIdx = 1
		}
		sShift := parent.children[sIdx].shift
		sNode := parent.children[sIdx].node

		if t.root == parent {
			// Parent is root → promote sibling.
			sNode.applyShift(sShift)
			t.root = sNode
			sNode.parent = nil
			sNode.parentID = 0
		} else {
			// Replace parent in grandparent with sibling.
			gp := parent.parent
			gpIdx := parent.parentID
			gp.children[gpIdx].node = sNode
			gp.children[gpIdx].shift += sShift
			sNode.parent = gp
			sNode.parentID = gpIdx

			if sIdx == 1 {
				// Update grandparent pivot.
				smallest := findSmallestKey(sNode)
				if gpIdx == 0 {
					updateSmallestKey(gp, smallest)
				} else {
					gp.pivots[gpIdx-1] = dupKey(smallest)
				}
			}
		}

		if t.nodeIsEmpty(parent) {
			t.recycleNode(parent)
		}
	} else {
		// Normal case: remove the child slot.
		if pIdx == 0 {
			copy(parent.pivots[:parent.count-1], parent.pivots[1:parent.count])
			copy(parent.children[:parent.count], parent.children[1:parent.count+1])
			parent.count--
			for i := uint32(0); i < parent.count+1; i++ {
				parent.children[i].node.parentID--
			}
			updateSmallestKey(parent, findSmallestKey(parent))
		} else {
			copy(parent.pivots[pIdx-1:parent.count-1], parent.pivots[pIdx:parent.count])
			copy(parent.children[pIdx:parent.count], parent.children[pIdx+1:parent.count+1])
			parent.count--
			for i := pIdx; i < parent.count+1; i++ {
				parent.children[i].node.parentID--
			}
		}

		if t.nodeIsEmpty(parent) {
			t.recycleNode(parent)
		}
	}
}

// applyShift adds shift to all loff values in a subtree (used when collapsing).
func (node *treeNode) applyShift(shift int64) {
	if node.isLeaf {
		for i := uint32(0); i < node.count; i++ {
			node.anchors[i].loff = uint32(int64(node.anchors[i].loff) + shift)
		}
	} else {
		for i := uint32(0); i < node.count+1; i++ {
			node.children[i].shift += shift
		}
	}
}

// nextAnchor finds the anchor for key using nh as a hint (avoids full descent
// when sequentially iterating). Sets nh.node, nh.idx, and nh.shift.
func (t *dbTree) nextAnchor(nh *nodeHandler, key []byte) {
	if nh.node == nil {
		*nh = t.findAnchorPos(key)
		return
	}
	// Walk up until we find an ancestor that contains key.
	node := nh.node
	cur := nh.node
	for cur.parent != nil {
		pIdx := cur.parentID
		if pIdx+1 > cur.parent.count {
			// Rightmost child — go up.
			cur = cur.parent
		} else if compareKeys(key, cur.parent.pivots[pIdx]) < 0 {
			// Key belongs in the subtree rooted at cur.
			node = cur
			break
		} else {
			node = cur.parent
			cur = node
		}
	}
	// Descend from node.
	for !node.isLeaf {
		idx := findPosInInternal(node, key)
		node = node.children[idx].node
	}
	nh.node = node
	nh.idx = findPosInLeafLE(node, key)
	accumulatedShift(nh)
}

// dupKey duplicates a byte slice.
func dupKey(k []byte) []byte {
	d := make([]byte, len(k))
	copy(d, k)
	return d
}
