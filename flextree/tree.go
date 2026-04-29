// Package flextree implements a mmap-backed B+tree for logical-to-physical
// address translation. It supports "shifting" inserts and deletes that move
// all subsequent logical extents, enabling an append-only storage model with
// in-place logical reuse. The tree can be persisted to disk and reloaded.
package flextree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"unsafe"
)

var leafSlabPool = sync.Pool{
	New: func() any {
		b := make([]byte, slabSize*unsafe.Sizeof(node{}))
		return &b
	},
}

var internalSlabPool = sync.Pool{
	New: func() any {
		b := make([]byte, slabSize*unsafe.Sizeof(node{}))
		return &b
	},
}

var (
	// ErrExtentTooLarge indicates an insert extent exceeds the configured maximum extent size.
	ErrExtentTooLarge = errors.New("extent size too large")
	// ErrOutOfRange indicates a requested logical offset range is outside current tree bounds.
	ErrOutOfRange = errors.New("offset out of range")
	// ErrCorruptedTree indicates the tree structure is corrupted (e.g. a cycle was detected).
	ErrCorruptedTree = errors.New("corrupted tree structure detected")
	// ErrTagNotFound indicates no tag exists at the requested logical offset.
	ErrTagNotFound = errors.New("tag not found")
)

// Tree is an extent-mapped B-tree for logical-to-physical address translation.
// It supports "shifting" insertions and deletions, where inserting at an offset
// moves all subsequent mappings in logical space.
type Tree struct {
	maxLoff       uint64
	maxExtentSize uint32
	// extentMask is (maxExtentSize - 1) when maxExtentSize is a power of two,
	// allowing a cheap XOR same-block check instead of division. Zero otherwise.
	extentMask uint64
	version    uint64

	root     *node
	leafHead *node
	freeIDs  []uint64

	// Go specific optimizations
	nodeCount uint64
	maxNodeID uint64
	rootID    uint64

	freeLeafNodes     []*node
	freeInternalNodes []*node

	leafSlabs     [][]node
	internalSlabs [][]node
	leafRaw       []*[]byte
	internalRaw   []*[]byte
	nextLeaf      int
	nextInternal  int

	// Last-accessed leaf cache (Implicit Search Fast-Paths)
	// lastLeafValid is a bool (not pointer nil-check) to avoid GC write barriers
	// on cache invalidation — scalar writes do not trigger write barriers.
	lastLeaf      *node
	lastLeafValid bool
	lastPath      path
	lastBase      uint64
}

const (
	slabSize = 4096
	nodeSize = 1024

	treeMetaMagic   = 0x46545245 // 'FTRE'
	treeMetaVersion = 1
)

// NewTree creates an empty tree with the given maximum extent size.
// maxExtentSize defines the largest contiguous extent that can be stored.
func NewTree(maxExtentSize uint32) *Tree {
	mask := uint64(0)
	if maxExtentSize&(maxExtentSize-1) == 0 {
		mask = uint64(maxExtentSize) - 1
	}
	t := &Tree{
		maxExtentSize:     maxExtentSize,
		extentMask:        mask,
		freeIDs:           make([]uint64, 0, 1024),
		freeLeafNodes:     make([]*node, 0, 128),
		freeInternalNodes: make([]*node, 0, 128),
		leafSlabs:         make([][]node, 0, 16),
		internalSlabs:     make([][]node, 0, 16),
		leafRaw:           make([]*[]byte, 0, 16),
		internalRaw:       make([]*[]byte, 0, 16),
		nextLeaf:          slabSize,
		nextInternal:      slabSize,
	}
	t.root = t.newNode(true)
	t.leafHead = t.root
	t.rootID = t.root.id
	return t
}

// Close releases resources associated with the tree back to the global pools.
func (t *Tree) Close() {
	for _, b := range t.leafRaw {
		leafSlabPool.Put(b)
	}
	for _, b := range t.internalRaw {
		internalSlabPool.Put(b)
	}
	t.leafRaw = nil
	t.internalRaw = nil
	t.leafSlabs = nil
	t.internalSlabs = nil
}

func (t *Tree) allocID() uint64 {
	if len(t.freeIDs) > 0 {
		id := t.freeIDs[len(t.freeIDs)-1]
		t.freeIDs = t.freeIDs[:len(t.freeIDs)-1]
		return id
	}
	id := t.maxNodeID
	t.maxNodeID++
	return id
}

func (t *Tree) newNode(isLeaf bool) *node {
	var n *node
	if isLeaf {
		if len(t.freeLeafNodes) > 0 {
			n = t.freeLeafNodes[len(t.freeLeafNodes)-1]
			t.freeLeafNodes = t.freeLeafNodes[:len(t.freeLeafNodes)-1]
		} else {
			if t.nextLeaf >= slabSize {
				slabPtr := leafSlabPool.Get().(*[]byte)
				t.leafRaw = append(t.leafRaw, slabPtr)
				t.leafSlabs = append(t.leafSlabs, *(*[]node)(unsafe.Pointer(slabPtr)))
				t.nextLeaf = 0
			}
			n = &t.leafSlabs[len(t.leafSlabs)-1][t.nextLeaf]
			t.nextLeaf++
		}
	} else {
		if len(t.freeInternalNodes) > 0 {
			n = t.freeInternalNodes[len(t.freeInternalNodes)-1]
			t.freeInternalNodes = t.freeInternalNodes[:len(t.freeInternalNodes)-1]
		} else {
			if t.nextInternal >= slabSize {
				slabPtr := internalSlabPool.Get().(*[]byte)
				t.internalRaw = append(t.internalRaw, slabPtr)
				t.internalSlabs = append(t.internalSlabs, *(*[]node)(unsafe.Pointer(slabPtr)))
				t.nextInternal = 0
			}
			n = &t.internalSlabs[len(t.internalSlabs)-1][t.nextInternal]
			t.nextInternal++
		}
	}
	n.id = t.allocID()
	n.isLeaf = isLeaf
	n.tree = t
	n.count = 0
	n.dirty = false
	t.nodeCount++
	return n
}

func (t *Tree) freeNode(n *node) {
	if t.lastLeaf == n {
		t.lastLeafValid = false
	}
	t.freeIDs = append(t.freeIDs, n.id)
	if n.isLeaf {
		t.freeLeafNodes = append(t.freeLeafNodes, n)
	} else {
		t.freeInternalNodes = append(t.freeInternalNodes, n)
	}
	t.nodeCount--
}

// MaxLoff returns the current maximum logical offset mapped by the tree.
func (t *Tree) MaxLoff() uint64 {
	return t.maxLoff
}

// Insert inserts a new physical extent mapping into the tree at the given
// logical offset. This operation shifts all subsequent logical mappings.
func (t *Tree) Insert(loff, poff uint64, length uint32) error {
	if length == 0 {
		return nil
	}

	// Super-fast path: check if we can append directly (50% of typical workloads)
	if loff == t.maxLoff && t.lastLeafValid {
		node := t.lastLeaf
		relLoff := loff - t.lastBase
		ld := node.leaf()

		// Fast merging check
		if node.count > 0 {
			last := &ld.extents[node.count-1]
			if uint32(relLoff) == last.loff+last.len {
				lastPoff := last.poff()
				if last.tag() == 0 && lastPoff+uint64(last.len) == poff &&
					last.len+length <= t.maxExtentSize {
					sameBlock := false
					if t.extentMask != 0 {
						sameBlock = (lastPoff^poff)&^t.extentMask == 0
					} else {
						sameBlock = lastPoff/uint64(t.maxExtentSize) == poff/uint64(t.maxExtentSize)
					}
					if sameBlock {
						last.len += length
						node.dirty = true
						t.maxLoff += uint64(length)
						return nil
					}
				}
				// Simple append to this leaf if space exists
				if node.count < leafCap {
					next := &ld.extents[node.count]
					next.loff = uint32(relLoff)
					next.len = length
					next.setPoffTag(poff, 0)
					node.count++
					node.dirty = true
					t.maxLoff += uint64(length)
					return nil
				}
			}
		}
	}

	if loff > t.maxLoff {
		holeLen := loff - t.maxLoff
		for holeLen > 0 {
			tsize := min(uint32(holeLen), t.maxExtentSize)
			if err := t.insertR(t.maxLoff, holeBit, tsize, 0); err != nil {
				return err
			}
			holeLen -= uint64(tsize)
		}
	}

	return t.insertR(loff, poff, length, 0)
}

// InsertWithTag inserts a new extent mapping with an associated 16-bit tag.
func (t *Tree) InsertWithTag(loff, poff uint64, length uint32, tag uint16) error {
	if length == 0 {
		return nil
	}
	if loff > t.maxLoff {
		holeLen := loff - t.maxLoff
		for holeLen > 0 {
			tsize := min(uint32(holeLen), t.maxExtentSize)
			if err := t.insertR(t.maxLoff, holeBit, tsize, 0); err != nil {
				return err
			}
			holeLen -= uint64(tsize)
		}
	}
	return t.insertR(loff, poff, length, tag)
}

func (t *Tree) insertR(loff, poff uint64, length uint32, tag uint16) error {
	if length > t.maxExtentSize {
		return ErrExtentTooLarge
	}

	needPropagate := loff != t.maxLoff

	var p path
	t.findLeafNode(loff, &p)
	if p.level == 255 {
		return ErrCorruptedTree
	}
	node := p.nodes[p.level]

	node.insertToLeaf(uint32(p.oloff), poff, length, tag)

	if p.level > 0 {
		node.rebase(&p)
	}
	node.dirty = true
	if needPropagate {
		node.shiftUpPropagate(&p, int64(length))
	}

	t.maxLoff += uint64(length)

	if node.isFull() {
		node.splitLeaf(&p)
		t.lastLeafValid = false
	}

	return nil
}

// InsertAppend inserts a new extent at the end of the logical address space.
func (t *Tree) InsertAppend(poff uint64, length uint32) error {
	return t.Insert(t.maxLoff, poff, length)
}

// Delete removes a logical range from the tree, shifting following extents left.
func (t *Tree) Delete(loff, length uint64) error {
	if loff+length > t.maxLoff {
		return ErrOutOfRange
	}
	t.lastLeafValid = false

	olen := length
	for olen > 0 {
		var p path
		t.findLeafNode(loff, &p)
		if p.level == 255 {
			return ErrCorruptedTree
		}
		tloff := p.oloff
		node := p.nodes[p.level]

		target := findPosInLeaf(node, uint32(tloff))
		if target == node.count {
			if target > 0 {
				target--
			} else {
				// Empty leaf or tloff out of bounds, shouldn't happen with valid loff
				break
			}
		}
		ld := node.leaf()

		currExtent := &ld.extents[target]

		tlen := min(uint64(currExtent.loff+currExtent.len)-tloff, olen)
		if tlen == 0 {
			// loff is exactly at the end of this extent; nothing to delete here.
			break
		}

		shift := uint32(1)
		if uint64(currExtent.loff) == tloff {
			currExtent.len -= uint32(tlen)
			currExtent.setPoff(currExtent.poff() + tlen)
			currExtent.setTag(0)
			if currExtent.len == 0 {
				copy(ld.extents[target:node.count-1], ld.extents[target+1:node.count])
				node.count--
				shift = 0
			}
		} else {
			tmp := uint32(tloff - uint64(currExtent.loff))
			if currExtent.len-tmp == uint32(tlen) {
				currExtent.len -= uint32(tlen)
			} else {
				right := extent{
					loff: uint32(tloff) + uint32(tlen),
					len:  currExtent.len - tmp - uint32(tlen),
				}
				right.setPoff(currExtent.poff() + uint64(tmp) + tlen)
				right.setTag(0)

				copy(ld.extents[target+2:], ld.extents[target+1:node.count])
				currExtent.len = tmp
				ld.extents[target+1] = right
				node.count++
			}
		}

		for i := target + shift; i < node.count; i++ {
			ld.extents[i].loff -= uint32(tlen)
		}

		node.dirty = true
		node.shiftUpPropagate(&p, -int64(tlen))

		olen -= tlen
		t.maxLoff -= tlen

		if node.isFull() {
			node.splitLeaf(&p)
		} else if node.isEmpty() {
			node.recycle(&p)
		}
	}

	t.lastLeafValid = false
	return nil
}

// QueryResult represents a physical mapping returned by a Query.
type QueryResult struct {
	// Poff is the physical offset of this mapping. A value with holeBit set
	// indicates a logical hole; use IsHole() to test this.
	Poff uint64
	// Len is the number of bytes covered by this mapping.
	Len uint64
}

// IsHole reports whether the query result represents a logical hole.
func (r QueryResult) IsHole() bool {
	return r.Poff&holeBit != 0
}

// Query returns all physical extents covering the given logical range.
// It populates the provided buf if possible to avoid allocations.
func (t *Tree) Query(loff, length uint64, buf []QueryResult) []QueryResult {
	if loff+length > t.maxLoff {
		return nil
	}

	res := buf[:0]
	tlen := length

	// Optimized leaf-only traversal
	node, relLoff := t.findLeaf(loff)
	if node == nil {
		return nil
	}
	ld := node.leaf()
	idx := findPosInLeaf(node, uint32(relLoff))

	for tlen > 0 {
		if idx >= node.count {
			node = ld.next
			if node == nil {
				break
			}
			ld = node.leaf()
			idx = 0
			if node.count == 0 {
				break
			}
			relLoff = uint64(ld.extents[0].loff)
		}

		ext := &ld.extents[idx]
		if relLoff < uint64(ext.loff) {
			step := min(uint64(ext.loff)-relLoff, tlen)
			tlen -= step
			relLoff += step
			if tlen == 0 {
				break
			}
		}

		diff := relLoff - uint64(ext.loff)
		step := min(uint64(ext.len)-diff, tlen)

		res = append(res, QueryResult{
			Poff: ext.poff() + uint64(diff),
			Len:  step,
		})

		tlen -= step
		relLoff += step
		idx++
	}

	return res
}

// findLeaf is a faster version of findLeafNode that returns only the leaf and relLoff.
// It DOES NOT update the tree's traversal path cache.
func (t *Tree) findLeaf(loff uint64) (*node, uint64) {
	if t.lastLeafValid && loff >= t.lastBase {
		node := t.lastLeaf
		relLoff := loff - t.lastBase
		if node.isLeaf {
			count := node.count
			if count > 0 {
				ld := node.leaf()
				// BCE hint
				_ = ld.extents[leafCap]
				endLoff := uint64(ld.extents[count-1].loff + ld.extents[count-1].len)
				if relLoff >= uint64(ld.extents[0].loff) &&
					(relLoff < endLoff || (loff == t.maxLoff && relLoff == endLoff)) {
					return node, relLoff
				}
			}
		}
	}

	node := t.root
	l := loff
	depth := 0
	for !node.isLeaf {
		if depth >= pathDepth {
			return nil, 0 // corruption: cycle detected
		}
		in := node.internal()
		count := node.count
		// BCE hints
		_ = in.pivots[internalCap-1]
		_ = in.children[internalCap]

		target := findPosInInternal(in, count, l)
		ce := &in.children[target]

		l -= uint64(ce.shift)
		node = ce.node
		depth++
	}

	t.lastLeaf = node
	t.lastLeafValid = true
	t.lastBase = loff - l
	// Invalidate the full path cache as we didn't track it
	t.lastPath.level = 255
	return node, l
}

func (t *Tree) findLeafNode(loff uint64, p *path) {
	if t.lastLeafValid && t.lastPath.level != 255 && loff >= t.lastBase {
		// Potential cache hit
		node := t.lastLeaf
		relLoff := loff - t.lastBase
		if node.isLeaf {
			count := node.count
			if count > 0 {
				ld := node.leaf()
				endLoff := uint64(ld.extents[count-1].loff + ld.extents[count-1].len)
				if uint64(relLoff) >= uint64(ld.extents[0].loff) &&
					(uint64(relLoff) < endLoff ||
						(loff == t.maxLoff && uint64(relLoff) == endLoff)) {
					*p = t.lastPath
					p.oloff = relLoff
					return
				}
			}
		}
	}

	p.level = 0
	node := t.root
	l := loff
	if node.isLeaf {
		p.nodes[0] = node
		t.lastLeaf = node
		t.lastLeafValid = true
		t.lastPath = *p
		t.lastBase = loff - l
		p.oloff = l
		return
	}

	for {
		if p.level >= pathDepth {
			p.level = 255 // sentinel: corrupted tree, cycle detected
			return
		}
		in := node.internal()
		count := node.count

		target := findPosInInternal(in, count, l)

		ce := &in.children[target]

		p.nodes[p.level] = node
		p.path[p.level] = uint8(target)
		p.level++

		l -= uint64(ce.shift)
		node = ce.node
		if node.isLeaf {
			p.nodes[p.level] = node
			t.lastLeaf = node
			t.lastLeafValid = true
			t.lastPath = *p
			t.lastBase = loff - l
			p.oloff = l
			return
		}
	}
}

// SetTag associates a tag with the extent at the given logical offset.
func (t *Tree) SetTag(oloff uint64, tag uint16) error {
	if oloff >= t.maxLoff {
		return ErrOutOfRange
	}

	var p path
	t.findLeafNode(oloff, &p)
	relLoff := p.oloff
	node := p.nodes[p.level]
	target := findPosInLeaf(node, uint32(relLoff))
	if target >= node.count {
		return ErrOutOfRange
	}

	ld := node.leaf()
	currExtent := &ld.extents[target]

	if uint64(currExtent.loff) == relLoff {
		currExtent.setTag(tag)
	} else {
		if node.count >= leafCap {
			return fmt.Errorf("flextree: SetTag failed: leaf node full, cannot split extent")
		}
		so := uint32(relLoff - uint64(currExtent.loff))
		copy(ld.extents[target+2:node.count+1], ld.extents[target+1:node.count])

		left := extent{loff: currExtent.loff, len: so, pTag: currExtent.pTag}
		right := extent{
			loff: uint32(relLoff),
			len:  currExtent.len - so,
		}
		right.setPoff(currExtent.poff() + uint64(so))
		right.setTag(tag)

		ld.extents[target] = left
		ld.extents[target+1] = right
		node.count++
	}

	node.dirty = true
	t.lastLeafValid = false
	if node.isFull() {
		node.splitLeaf(&p)
	}

	return nil
}

// UpdatePoff re-maps the physical offset of the extent at the given logical offset.
func (t *Tree) UpdatePoff(oloff, poff uint64, length uint32) error {
	if oloff >= t.maxLoff {
		return ErrOutOfRange
	}

	node, relLoff := t.findLeaf(oloff)
	if node == nil {
		return ErrCorruptedTree
	}
	target := findPosInLeaf(node, uint32(relLoff))
	if target >= node.count {
		return ErrOutOfRange
	}

	ld := node.leaf()
	currExtent := &ld.extents[target]

	if uint64(currExtent.loff) == relLoff && currExtent.len == length {
		currExtent.setPoff(poff)
		node.dirty = true
		t.lastLeafValid = false
		return nil
	}

	return fmt.Errorf("UpdatePoff mismatch: loff=%d, len=%d, found_loff=%d, found_len=%d",
		oloff, length, uint64(currExtent.loff)+t.lastBase, currExtent.len)
}

// GetTag retrieves the tag associated with the logical offset.
func (t *Tree) GetTag(oloff uint64) (uint16, error) {
	if oloff >= t.maxLoff {
		return 0, ErrOutOfRange
	}

	node, relLoff := t.findLeaf(oloff)
	if node == nil {
		return 0, ErrCorruptedTree
	}
	target := findPosInLeaf(node, uint32(relLoff))
	if target >= node.count {
		return 0, ErrOutOfRange
	}

	ld := node.leaf()
	currExtent := &ld.extents[target]
	if uint64(currExtent.loff) == relLoff && currExtent.tag() != 0 {
		return currExtent.tag(), nil
	}
	return 0, ErrTagNotFound
}

// CheckInvariants verifies the internal consistency of the tree.
func (t *Tree) CheckInvariants() error {
	return t.checkNode(t.root, 0, t.maxLoff)
}

func (t *Tree) checkNode(n *node, minLoff, maxLoff uint64) error {
	if n.isLeaf {
		ld := n.leaf()
		var last uint32
		for i := uint32(0); i < n.count; i++ {
			if i > 0 && ld.extents[i].loff < last {
				return fmt.Errorf("unsorted extents in leaf %d", n.id)
			}
			last = ld.extents[i].loff + ld.extents[i].len
		}
	} else {
		in := n.internal()
		for i := uint32(0); i < n.count; i++ {
			if i > 0 && in.pivots[i] <= in.pivots[i-1] {
				return fmt.Errorf("unsorted pivots in internal %d", n.id)
			}
			childMin := minLoff
			if i > 0 {
				childMin = in.pivots[i-1]
			}
			if err := t.checkNode(in.children[i].node, childMin, in.pivots[i]); err != nil {
				return err
			}
		}
		childMin := minLoff
		if n.count > 0 {
			childMin = in.pivots[n.count-1]
		}
		if err := t.checkNode(in.children[n.count].node, childMin, maxLoff); err != nil {
			return err
		}
	}
	return nil
}

// IterateExtents calls fn for each extent mapping in the tree.
func (t *Tree) IterateExtents(fn func(loff, poff uint64, length uint32, tag uint16) bool) {
	t.iterateNodeRec(t.root, 0, fn)
}

func (t *Tree) iterateNodeRec(n *node, loff uint64, fn func(loff, poff uint64, length uint32, tag uint16) bool) bool {
	if n.isLeaf {
		ld := n.leaf()
		for i := uint32(0); i < n.count; i++ {
			ext := &ld.extents[i]
			if !fn(loff+uint64(ext.loff), ext.poff(), ext.len, ext.tag()) {
				return false
			}
		}
		return true
	}

	in := n.internal()
	for i := uint32(0); i <= n.count; i++ {
		if !t.iterateNodeRec(in.children[i].node, loff+uint64(in.children[i].shift), fn) {
			return false
		}
	}
	return true
}

// Sync persists the tree state to files using copy-on-write semantics.
// Dirty nodes are written to new positions before the metadata is updated,
// so a crash mid-sync leaves the previous committed state intact.
func (t *Tree) Sync(metaPath, nodePath string) error {
	nodeFile, err := os.OpenFile(nodePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer nodeFile.Close()

	// COW pass: assign new IDs to dirty nodes (post-order so children are
	// updated before parents), write each dirty node at its new position,
	// and collect the old IDs to free after the commit.
	var oldIDs []uint64
	if _, err := t.syncCOW(t.root, nodeFile, &oldIDs); err != nil {
		return err
	}

	if err := nodeFile.Sync(); err != nil {
		return err
	}

	// Write metadata to a temporary file then rename for atomicity.
	tmpMetaPath := metaPath + ".tmp"
	metaFile, err := os.OpenFile(tmpMetaPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		metaFile.Close()
		_ = os.Remove(tmpMetaPath)
	}()

	t.version++

	metaBuf := make([]byte, 4096)
	binary.LittleEndian.PutUint32(metaBuf[0:4], treeMetaMagic)
	binary.LittleEndian.PutUint32(metaBuf[4:8], treeMetaVersion)
	binary.LittleEndian.PutUint64(metaBuf[8:16], t.maxLoff)
	binary.LittleEndian.PutUint32(metaBuf[16:20], t.maxExtentSize)
	binary.LittleEndian.PutUint64(metaBuf[20:28], t.version)
	binary.LittleEndian.PutUint64(metaBuf[28:36], t.nodeCount)
	binary.LittleEndian.PutUint64(metaBuf[36:44], t.maxNodeID)
	binary.LittleEndian.PutUint64(metaBuf[44:52], t.rootID)

	c := crc32.ChecksumIEEE(metaBuf[:52])
	binary.LittleEndian.PutUint32(metaBuf[52:56], c)

	if _, err := metaFile.Write(metaBuf); err != nil {
		return err
	}
	if err := metaFile.Sync(); err != nil {
		return err
	}
	if err := metaFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpMetaPath, metaPath); err != nil {
		return err
	}

	// Commit complete: old IDs are now unreferenced and can be reused.
	t.freeIDs = append(t.freeIDs, oldIDs...)
	return nil
}

// syncCOW performs a post-order traversal, assigns new IDs to dirty nodes,
// writes them to the node file, and returns whether this node was dirty.
// oldIDs accumulates the superseded IDs to be freed after the commit.
func (t *Tree) syncCOW(n *node, f *os.File, oldIDs *[]uint64) (bool, error) {
	dirty := n.dirty

	if !n.isLeaf {
		in := n.internal()
		for i := 0; i <= int(n.count); i++ {
			childDirty, err := t.syncCOW(in.children[i].node, f, oldIDs)
			if err != nil {
				return false, err
			}
			if childDirty {
				dirty = true
				in.childrenIDs[i] = in.children[i].node.id
			}
		}
	}

	if !dirty {
		return false, nil
	}

	// Assign a new ID so the old slot is not overwritten until after commit.
	*oldIDs = append(*oldIDs, n.id)
	n.id = t.allocID()
	if n == t.root {
		t.rootID = n.id
	}

	if err := t.writeNode(n, f); err != nil {
		return false, err
	}
	n.dirty = false
	return true, nil
}

func (t *Tree) writeNode(n *node, f *os.File) error {
	buf := make([]byte, nodeSize)
	binary.LittleEndian.PutUint32(buf[0:4], n.count)
	if n.isLeaf {
		buf[4] = 1
	} else {
		buf[4] = 0
	}
	binary.LittleEndian.PutUint64(buf[8:16], n.id)

	if n.isLeaf {
		ld := n.leaf()
		for i := 0; i < int(n.count); i++ {
			off := 32 + i*16
			binary.LittleEndian.PutUint32(buf[off:off+4], ld.extents[i].loff)
			binary.LittleEndian.PutUint32(buf[off+4:off+8], ld.extents[i].len)
			binary.LittleEndian.PutUint64(buf[off+8:off+16], ld.extents[i].pTag)
		}
	} else {
		in := n.internal()
		for i := 0; i < int(n.count); i++ {
			off := 32 + i*8
			binary.LittleEndian.PutUint64(buf[off:off+8], in.pivots[i])
		}
		for i := 0; i <= int(n.count); i++ {
			off := 32 + int(internalCap)*8 + i*16
			binary.LittleEndian.PutUint64(buf[off:off+8], uint64(in.children[i].shift))
			binary.LittleEndian.PutUint64(buf[off+8:off+16], in.childrenIDs[i])
		}
	}

	_, err := f.WriteAt(buf, int64(n.id*nodeSize))
	return err
}

// LoadTree loads a tree from the specified metadata and node files.
func LoadTree(metaPath, nodePath string) (*Tree, error) {
	metaFile, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer metaFile.Close()

	nodeFile, err := os.Open(nodePath)
	if err != nil {
		return nil, err
	}
	defer nodeFile.Close()

	// Read both meta pages and pick the one with the highest valid version.
	metaBuf := make([]byte, 8192)
	n, _ := metaFile.ReadAt(metaBuf, 0)

	var bestMeta []byte
	var maxVer uint64
	foundValid := false

	for i := 0; i < 2; i++ {
		off := i * 4096
		if off+56 > n {
			continue
		}
		p := metaBuf[off : off+4096]
		magic := binary.LittleEndian.Uint32(p[0:4])
		if magic != treeMetaMagic {
			continue
		}

		// Verify checksum.
		storedC := binary.LittleEndian.Uint32(p[52:56])
		if crc32.ChecksumIEEE(p[:52]) != storedC {
			continue
		}

		ver := binary.LittleEndian.Uint64(p[20:28])
		if ver >= maxVer {
			maxVer = ver
			bestMeta = p
			foundValid = true
		}
	}

	if !foundValid {
		// Try legacy format only if no versioned meta found.
		if n >= 48 {
			bestMeta = metaBuf[0:4096]
		} else {
			return nil, fmt.Errorf("flextree: no valid metadata found")
		}
	}

	var maxLoff, version, nodeCount, maxNodeID, rootID uint64
	var mes uint32

	magic := binary.LittleEndian.Uint32(bestMeta[0:4])
	if magic == treeMetaMagic {
		v := binary.LittleEndian.Uint32(bestMeta[4:8])
		if v > treeMetaVersion {
			return nil, fmt.Errorf("flextree: unsupported metadata version %d", v)
		}
		maxLoff = binary.LittleEndian.Uint64(bestMeta[8:16])
		mes = binary.LittleEndian.Uint32(bestMeta[16:20])
		version = binary.LittleEndian.Uint64(bestMeta[20:28])
		nodeCount = binary.LittleEndian.Uint64(bestMeta[28:36])
		maxNodeID = binary.LittleEndian.Uint64(bestMeta[36:44])
		rootID = binary.LittleEndian.Uint64(bestMeta[44:52])
	} else {
		// Legacy format.
		maxLoff = binary.LittleEndian.Uint64(bestMeta[0:8])
		mes = binary.LittleEndian.Uint32(bestMeta[8:12])
		version = binary.LittleEndian.Uint64(bestMeta[12:20])
		nodeCount = binary.LittleEndian.Uint64(bestMeta[20:28])
		maxNodeID = binary.LittleEndian.Uint64(bestMeta[28:36])
		rootID = binary.LittleEndian.Uint64(bestMeta[36:44])
	}

	mask := uint64(0)
	if mes&(mes-1) == 0 {
		mask = uint64(mes) - 1
	}
	t := &Tree{
		maxLoff:       maxLoff,
		maxExtentSize: mes,
		extentMask:    mask,
		version:       version,
		nodeCount:     nodeCount,
		maxNodeID:     maxNodeID,
		rootID:        rootID,
		freeIDs:       make([]uint64, 0),
		leafSlabs:     make([][]node, 0),
		internalSlabs: make([][]node, 0),
		nextLeaf:      slabSize,
		nextInternal:  slabSize,
	}

	t.root, err = t.loadNodeRec(t.rootID, nodeFile, make(map[uint64]bool))
	if err != nil {
		return nil, err
	}

	// Rebuild linked list
	var last *node
	t.rebuildLinkedList(t.root, &last)

	// Rebuild free ID list: any ID in [0, maxNodeID) not in the loaded tree is free.
	usedIDs := make(map[uint64]struct{}, t.nodeCount)
	collectNodeIDs(t.root, usedIDs)
	for id := uint64(0); id < t.maxNodeID; id++ {
		if _, ok := usedIDs[id]; !ok {
			t.freeIDs = append(t.freeIDs, id)
		}
	}

	return t, nil
}

func collectNodeIDs(n *node, ids map[uint64]struct{}) {
	ids[n.id] = struct{}{}
	if !n.isLeaf {
		in := n.internal()
		for i := 0; i <= int(n.count); i++ {
			collectNodeIDs(in.children[i].node, ids)
		}
	}
}

func (t *Tree) loadNodeRec(id uint64, f *os.File, seen map[uint64]bool) (*node, error) {
	if seen[id] {
		return nil, fmt.Errorf("flextree: circular dependency detected at node %d", id)
	}
	seen[id] = true

	buf := make([]byte, nodeSize)
	if _, err := f.ReadAt(buf, int64(id*nodeSize)); err != nil {
		return nil, err
	}

	count := binary.LittleEndian.Uint32(buf[0:4])
	isLeaf := buf[4] == 1
	n := t.newNodeExplicit(isLeaf, id)
	n.count = count

	if isLeaf {
		ld := n.leaf()
		for i := 0; i < int(count); i++ {
			off := 32 + i*16
			ld.extents[i].loff = binary.LittleEndian.Uint32(buf[off : off+4])
			ld.extents[i].len = binary.LittleEndian.Uint32(buf[off+4 : off+8])
			ld.extents[i].pTag = binary.LittleEndian.Uint64(buf[off+8 : off+16])
		}
	} else {
		in := n.internal()
		for i := 0; i < int(count); i++ {
			off := 32 + i*8
			in.pivots[i] = binary.LittleEndian.Uint64(buf[off : off+8])
		}
		for i := 0; i <= int(count); i++ {
			off := 32 + int(internalCap)*8 + i*16
			in.children[i].shift = int64(binary.LittleEndian.Uint64(buf[off : off+8]))
			in.childrenIDs[i] = binary.LittleEndian.Uint64(buf[off+8 : off+16])

			child, err := t.loadNodeRec(in.childrenIDs[i], f, seen)
			if err != nil {
				return nil, err
			}

			in.children[i].node = child
		}
	}

	return n, nil
}

func (t *Tree) newNodeExplicit(isLeaf bool, id uint64) *node {
	var n *node
	if isLeaf {
		if t.nextLeaf >= slabSize {
			t.leafSlabs = append(t.leafSlabs, make([]node, slabSize))
			t.nextLeaf = 0
		}
		n = &t.leafSlabs[len(t.leafSlabs)-1][t.nextLeaf]
		t.nextLeaf++
	} else {
		if t.nextInternal >= slabSize {
			t.internalSlabs = append(t.internalSlabs, make([]node, slabSize))
			t.nextInternal = 0
		}
		n = &t.internalSlabs[len(t.internalSlabs)-1][t.nextInternal]
		t.nextInternal++
	}
	n.id = id
	n.isLeaf = isLeaf
	n.tree = t
	return n
}

func (t *Tree) rebuildLinkedList(n *node, last **node) {
	if n.isLeaf {
		ld := n.leaf()
		if *last == nil {
			t.leafHead = n
			ld.prev = nil
		} else {
			(*last).leaf().next = n
			ld.prev = *last
		}
		ld.next = nil
		*last = n
	} else {
		in := n.internal()
		for i := 0; i <= int(n.count); i++ {
			t.rebuildLinkedList(in.children[i].node, last)
		}
	}
}
