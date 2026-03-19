package flextree

const (
	// leafCap is the maximum number of extents in a leaf node before split.
	leafCap = 60
	// internalCap is the maximum number of pivots in an internal node before split.
	internalCap = 30
	// pOffMask selects the low 48 bits used to store physical offset data.
	pOffMask = 0xFFFFFFFFFFFF
	// holeBit marks an extent as a logical hole in the physical address space.
	// External callers should use QueryResult.IsHole() rather than inspecting
	// this bit directly.
	holeBit = 1 << 47
)

// extent represents a contiguous range of logical offsets mapped to physical offsets.
// Matches C struct flextree_extent layout: loff(4), len(4), tag(2), poff(6).
type extent struct {
	loff uint32
	len  uint32
	pTag uint64 // Low 48 bits Poff, High 16 bits Tag
}

// poff returns the physical offset encoded in this extent.
func (e *extent) poff() uint64 {
	return e.pTag & pOffMask
}

// setPoff updates the physical offset encoded in this extent.
func (e *extent) setPoff(poff uint64) {
	e.pTag = (e.pTag & 0xFFFF000000000000) | (poff & pOffMask)
}

// tag returns the 16-bit tag encoded in this extent.
func (e *extent) tag() uint16 {
	return uint16(e.pTag >> 48)
}

// setTag updates the 16-bit tag encoded in this extent.
func (e *extent) setTag(tag uint16) {
	e.pTag = (e.pTag & pOffMask) | (uint64(tag) << 48)
}

// setPoffTag updates both physical offset and tag.
func (e *extent) setPoffTag(poff uint64, tag uint16) {
	e.pTag = (poff & pOffMask) | (uint64(tag) << 48)
}

// childEntry stores an internal child pointer and its accumulated logical shift.
type childEntry struct {
	node  *node
	shift int64
}
