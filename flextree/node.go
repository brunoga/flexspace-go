package flextree

import (
	"unsafe"
)

// pathDepth is the maximum traversal depth tracked in a path.
const pathDepth = 8

// path stores traversal state from root to a target node.
type path struct {
	level uint8
	path  [pathDepth]uint8
	nodes [pathDepth]*node
	oloff uint64
}

// parentIdx returns the index of the current node in its parent.
func (p *path) parentIdx() uint32 {
	if p.level == 0 {
		return ^uint32(0)
	}
	return uint32(p.path[p.level-1])
}

// node is the common header. Matches C struct flextree_node exactly.
type node struct {
	count  uint32
	isLeaf bool
	dirty  bool
	_      [2]byte // padding
	tree   *Tree

	// union { leafNode; internalNode }
	data [992]byte // 1024 - 32 (header)
	id   uint64
}

// leafNode overlay for the data union.
type leafNode struct {
	extents [leafCap + 1]extent
	prev    *node
	next    *node
}

// internalNode overlay for the data union.
type internalNode struct {
	pivots      [internalCap]uint64
	children    [internalCap + 1]childEntry
	childrenIDs [internalCap + 1]uint64
}

func (n *node) leaf() *leafNode {
	return (*leafNode)(unsafe.Pointer(&n.data))
}

func (n *node) internal() *internalNode {
	return (*internalNode)(unsafe.Pointer(&n.data))
}

func (n *node) isFull() bool {
	if n.isLeaf {
		return n.count >= leafCap
	}
	return n.count >= internalCap
}

func (n *node) isEmpty() bool {
	return n.count == 0
}

func findPosInLeaf(n *node, loff uint32) uint32 {
	count := n.count
	if count == 0 {
		return 0
	}

	ln := n.leaf()
	lo, hi := uint32(0), count
	for lo < hi {
		mid := (lo + hi) >> 1
		e := &ln.extents[mid]
		if e.loff <= loff {
			if e.loff+e.len > loff {
				return mid
			}
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func findPosInInternal(in *internalNode, count uint32, loff uint64) uint32 {
	hi := count
	lo := uint32(0)

	for lo < hi {
		mid := (lo + hi) >> 1
		if in.pivots[mid] <= loff {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (n *node) rebase(p *path) {
	if n.count == 0 {
		return
	}
	n.tree.lastLeafValid = false
	ln := n.leaf()
	extents := ln.extents[:n.count]

	maxAllowed := ^uint32(0) - n.tree.maxExtentSize*2
	if extents[n.count-1].loff >= maxAllowed {
		newBase := extents[0].loff
		pIdx := p.parentIdx()
		parent := p.nodes[p.level-1]
		in := parent.internal()
		in.children[pIdx].shift += int64(newBase)
		for i := uint32(0); i < n.count; i++ {
			extents[i].loff -= newBase
		}
		parent.dirty = true
		n.dirty = true
	}
}

func (n *node) shiftUpPropagate(p *path, shift int64) {
	n.tree.lastLeafValid = false
	for lvl := int(p.level); lvl > 0; lvl-- {
		parent := p.nodes[lvl-1]
		pIdx := uint32(p.path[lvl-1])
		count := parent.count
		if pIdx < count {
			in := parent.internal()
			for i := pIdx; i < count; i++ {
				in.pivots[i] = uint64(int64(in.pivots[i]) + shift)
				in.children[i+1].shift += shift
			}
			parent.dirty = true
		}
	}
}

func (n *node) shiftApply(shift int64) {
	n.tree.lastLeafValid = false
	count := n.count
	if n.isLeaf {
		ln := n.leaf()
		extents := ln.extents[:count]
		for i := range count {
			extents[i].loff = uint32(int64(extents[i].loff) + shift)
		}
	} else {
		in := n.internal()
		pivots := in.pivots[:count]
		children := in.children[:count+1]
		for i := range count {
			pivots[i] = uint64(int64(pivots[i]) + shift)
		}
		for i := uint32(0); i <= count; i++ {
			children[i].shift += shift
		}
	}
	n.dirty = true
}

func linkTwoNodes(n1, n2 *node) {
	ln1 := n1.leaf()
	ln2 := n2.leaf()
	ln2.prev = n1
	ln2.next = ln1.next
	ln1.next = n2
	if ln2.next != nil {
		ln2.next.leaf().prev = n2
	}
}

func (n *node) splitInternal(p *path) {
	n.tree.lastLeafValid = false
	n1 := n
	n2 := n.tree.newNode(false)

	in1 := n1.internal()
	in2 := n2.internal()

	count := (n1.count + 1) / 2
	newBase := in1.pivots[count]

	n2.count = n1.count - count - 1
	copy(in2.pivots[:n2.count], in1.pivots[count+1:])
	copy(in2.children[:n2.count+1], in1.children[count+1:])
	copy(in2.childrenIDs[:n2.count+1], in1.childrenIDs[count+1:])

	n1.count = count

	var parent *node
	if p.level == 0 {
		parent = n1.tree.newNode(false)
		n1.tree.root = parent
		n1.tree.rootID = parent.id
	} else {
		parent = p.nodes[p.level-1]
	}

	pin := parent.internal()
	if parent.count == 0 && parent == n1.tree.root && parent.id != n1.id && parent.id != n2.id {
		pin.children[0].node = n1
		pin.children[0].shift = 0
		pin.children[1].node = n2
		pin.children[1].shift = 0
		pin.pivots[0] = newBase
		pin.childrenIDs[0] = n1.id
		pin.childrenIDs[1] = n2.id
		parent.count = 1
	} else {
		pIdx := p.parentIdx()
		origShift := pin.children[pIdx].shift

		copy(pin.pivots[pIdx+1:], pin.pivots[pIdx:parent.count])
		copy(pin.children[pIdx+2:], pin.children[pIdx+1:parent.count+1])
		copy(pin.childrenIDs[pIdx+2:], pin.childrenIDs[pIdx+1:parent.count+1])

		pin.children[pIdx+1].node = n2
		pin.children[pIdx+1].shift = origShift
		pin.pivots[pIdx] = uint64(int64(newBase) + origShift)
		pin.childrenIDs[pIdx+1] = n2.id
		parent.count++
	}

	parent.dirty = true
	n1.dirty = true
	n2.dirty = true

	if parent.isFull() {
		ppath := *p
		ppath.level--
		parent.splitInternal(&ppath)
	}
}

func (n *node) splitLeaf(p *path) {
	n.tree.lastLeafValid = false
	n1 := n
	n2 := n.tree.newNode(true)

	linkTwoNodes(n1, n2)

	ln1 := n1.leaf()
	ln2 := n2.leaf()

	count := (n1.count + 1) / 2

	n2.count = n1.count - count
	copy(ln2.extents[:n2.count], ln1.extents[count:])

	n1.count = count

	var parent *node
	if p.level == 0 {
		parent = n1.tree.newNode(false)
		n1.tree.root = parent
		n1.tree.rootID = parent.id
	} else {
		parent = p.nodes[p.level-1]
	}

	pin := parent.internal()
	if parent.count == 0 {
		pin.children[0].node = n1
		pin.children[0].shift = 0
		pin.children[1].node = n2
		pin.children[1].shift = 0
		pin.pivots[0] = uint64(ln2.extents[0].loff)
		pin.childrenIDs[0] = n1.id
		pin.childrenIDs[1] = n2.id
		parent.count = 1
	} else {
		pIdx := p.parentIdx()
		origShift := pin.children[pIdx].shift

		copy(pin.pivots[pIdx+1:], pin.pivots[pIdx:parent.count])
		copy(pin.children[pIdx+2:], pin.children[pIdx+1:parent.count+1])
		copy(pin.childrenIDs[pIdx+2:], pin.childrenIDs[pIdx+1:parent.count+1])

		pin.children[pIdx+1].node = n2
		pin.children[pIdx+1].shift = origShift
		pin.pivots[pIdx] = uint64(int64(ln2.extents[0].loff) + origShift)
		pin.childrenIDs[pIdx+1] = n2.id
		parent.count++
	}

	parent.dirty = true
	n1.dirty = true
	n2.dirty = true

	if p.level > 0 {
		n1.rebase(p)
		spath := *p
		spath.path[spath.level-1]++
		n2.rebase(&spath)
	}

	if parent.isFull() {
		ppath := *p
		ppath.level--
		parent.splitInternal(&ppath)
	}
}

func (n *node) recycleLinkedList() {
	if n.tree.root == n {
		return
	}
	ln := n.leaf()
	prev, next := ln.prev, ln.next
	if prev != nil {
		prev.leaf().next = next
	} else {
		n.tree.leafHead = next
	}
	if next != nil {
		next.leaf().prev = prev
	}
}

func (n *node) recycle(p *path) {
	n.tree.lastLeafValid = false
	if n.tree.root == n {
		return
	}

	parent := p.nodes[p.level-1]
	pIdx := uint32(p.path[p.level-1])

	if n.isLeaf {
		n.recycleLinkedList()
	}
	n.tree.freeNode(n)

	pin := parent.internal()

	if parent.count == 0 {
		if n.tree.root == parent {
			newRoot := n.tree.newNode(true)
			n.tree.root = newRoot
			n.tree.rootID = newRoot.id
			n.tree.leafHead = newRoot
			n.tree.freeNode(parent)
		} else {
			ppath := *p
			ppath.level--
			parent.recycle(&ppath)
		}
		return
	}

	if parent.count == 1 {
		sIdx := uint32(0)
		if pIdx == 0 {
			sIdx = 1
		}
		sShift := pin.children[sIdx].shift
		sNode := pin.children[sIdx].node

		if n.tree.root == parent {
			sNode.shiftApply(sShift)
			n.tree.root = sNode
			n.tree.rootID = sNode.id
			n.tree.freeNode(parent)
		} else {
			gparent := p.nodes[p.level-2]
			gpIdx := uint32(p.path[p.level-2])
			gpin := gparent.internal()
			gpin.children[gpIdx].node = sNode
			gpin.children[gpIdx].shift += sShift
			gpin.childrenIDs[gpIdx] = sNode.id
			gparent.dirty = true
			n.tree.freeNode(parent)
		}
		return
	}

	if pIdx == 0 {
		copy(pin.pivots[:parent.count-1], pin.pivots[1:parent.count])
		copy(pin.children[:parent.count], pin.children[1:parent.count+1])
		copy(pin.childrenIDs[:parent.count], pin.childrenIDs[1:parent.count+1])
	} else {
		copy(pin.pivots[pIdx-1:], pin.pivots[pIdx:parent.count])
		copy(pin.children[pIdx:], pin.children[pIdx+1:parent.count+1])
		copy(pin.childrenIDs[pIdx:], pin.childrenIDs[pIdx+1:parent.count+1])
	}
	parent.count--
	parent.dirty = true
}

func extentSequential(e *extent, maxExtentSize uint32, extentMask uint64, loff uint32, poff uint64, len uint32) bool {
	ep := e.poff()
	if ep+uint64(e.len) != poff || e.loff+e.len != loff || e.len+len > maxExtentSize {
		return false
	}
	if extentMask != 0 {
		return (ep^poff)&^extentMask == 0
	}
	return ep/uint64(maxExtentSize) == poff/uint64(maxExtentSize)
}

func (n *node) insertToLeaf(loff uint32, poff uint64, length uint32, tag uint16) {
	ln := n.leaf()
	em := n.tree.extentMask
	mes := n.tree.maxExtentSize

	// Fast-path for sequential appends
	if n.count > 0 {
		last := &ln.extents[n.count-1]
		if loff == last.loff+last.len {
			// Inline extentSequential (using pow2 mask when available to avoid division)
			lastPoff := last.poff()
			if tag == 0 && last.tag() == 0 && lastPoff+uint64(last.len) == poff &&
				last.len+length <= mes {
				sameBlock := false
				if em != 0 {
					sameBlock = (lastPoff^poff)&^em == 0
				} else {
					sameBlock = lastPoff/uint64(mes) == poff/uint64(mes)
				}
				if sameBlock {
					last.len += length
					n.dirty = true
					return
				}
			}
			if n.count < leafCap {
				next := &ln.extents[n.count]
				next.loff = loff
				next.len = length
				next.setPoffTag(poff, tag)
				n.count++
				n.dirty = true
				return
			}
		}
	}

	target := findPosInLeaf(n, loff)
	shift := uint32(1)

	var t extent
	t.loff = loff
	t.len = length
	t.setPoffTag(poff, tag)

	if target == n.count {
		if target > 0 && tag == 0 && extentSequential(&ln.extents[target-1], mes, em, loff, poff, length) {
			ln.extents[target-1].len += length
			shift = 0
		} else {
			ln.extents[n.count] = t
			n.count++
		}
	} else {
		currExtent := &ln.extents[target]
		if currExtent.loff == loff {
			// Try merging with the previous extent when both extents are untagged
			// and physically sequential. The old extent at target (and everything
			// after it) still needs to be shifted forward by +length.
			if target > 0 && tag == 0 && ln.extents[target-1].tag() == 0 &&
				extentSequential(&ln.extents[target-1], mes, em, loff, poff, length) {
				ln.extents[target-1].len += length
				for i := int(target); i < int(n.count); i++ {
					ln.extents[i].loff += length
				}
				n.dirty = true
				return
			}
			copy(ln.extents[target+1:], ln.extents[target:n.count])
			ln.extents[target] = t
			n.count++
		} else { // split
			shift = 2
			so := loff - currExtent.loff
			copy(ln.extents[target+3:], ln.extents[target+1:n.count])

			left := extent{loff: currExtent.loff, len: so, pTag: currExtent.pTag}
			right := extent{loff: currExtent.loff + so, len: currExtent.len - so}
			right.setPoff(currExtent.poff() + uint64(so))

			ln.extents[target] = left
			ln.extents[target+2] = right
			ln.extents[target+1] = t
			n.count += 2
		}
	}

	if shift > 0 {
		count := n.count
		targetExtents := ln.extents[target+shift : count]
		for i := range targetExtents {
			targetExtents[i].loff += length
		}
	}
	n.dirty = true
}
