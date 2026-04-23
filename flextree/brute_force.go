package flextree

import (
	"errors"
	"slices"
)

// bruteForce is a simple reference implementation of the extent map used for
// correctness validation of Tree. It is intentionally unoptimised.
type bruteForce struct {
	maxExtentSize uint32
	maxLoff_      uint64
	extents       []bruteForceExtent
}

// bruteForceExtent represents a single extent in the brute-force reference.
type bruteForceExtent struct {
	Loff uint64
	Len  uint32
	Poff uint64
	Tag  uint16
}

// newBruteForce creates a brute-force reference instance.
func newBruteForce(maxExtentSize uint32) *bruteForce {
	return &bruteForce{
		maxExtentSize: maxExtentSize,
		extents:       make([]bruteForceExtent, 0, 1024),
	}
}

// Close is a no-op for the pure-Go implementation.
func (bf *bruteForce) Close() {}

// MaxLoff returns the maximum logical offset in the brute-force reference.
func (bf *bruteForce) MaxLoff() uint64 {
	return bf.maxLoff_
}

// Extents returns a copy of all extents.
func (bf *bruteForce) Extents() []bruteForceExtent {
	out := make([]bruteForceExtent, len(bf.extents))
	copy(out, bf.extents)
	return out
}

// findPos returns the index of the first extent that contains loff or starts
// after loff. Mirrors C brute_force_find_pos.
func (bf *bruteForce) findPos(loff uint64) int {
	n := len(bf.extents)
	lo, hi := 0, n
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if bf.extents[mid].Loff <= loff {
			lo = mid
		} else {
			hi = mid
		}
	}
	target := lo
	for target < n {
		e := &bf.extents[target]
		if (e.Loff <= loff && e.Loff+uint64(e.Len) > loff) || e.Loff > loff {
			break
		}
		target++
	}
	return target
}

func (bf *bruteForce) extentSequential(idx int, loff, poff uint64, length uint32) bool {
	e := &bf.extents[idx]
	return e.Poff+uint64(e.Len) == poff &&
		e.Loff+uint64(e.Len) == loff &&
		uint64(e.Len)+uint64(length) <= uint64(bf.maxExtentSize)
}

func (bf *bruteForce) insertR(loff, poff uint64, length uint32, tag uint16) error {
	if length == 0 {
		return nil
	}
	if length > bf.maxExtentSize {
		return ErrExtentTooLarge
	}
	if loff > bf.maxLoff_ {
		hlen := loff - bf.maxLoff_
		hloff := bf.maxLoff_
		for hlen > 0 {
			thlen := min(uint32(hlen), bf.maxExtentSize)
			if err := bf.insertR(hloff, holeBit, thlen, 0); err != nil {
				return err
			}
			hlen -= uint64(thlen)
			hloff += uint64(thlen)
		}
	}

	t := bruteForceExtent{Loff: loff, Len: length, Poff: poff & pOffMask, Tag: tag}
	target := bf.findPos(loff)
	n := len(bf.extents)
	shift := 1

	if target == n {
		if target > 0 && tag == 0 && bf.extentSequential(target-1, loff, poff, length) {
			bf.extents[target-1].Len += length
		} else {
			bf.extents = append(bf.extents, t)
		}
	} else if bf.extents[target].Loff == loff {
		if target > 0 && tag == 0 && bf.extentSequential(target-1, loff, poff, length) {
			bf.extents[target-1].Len += length
			shift = 0
		} else {
			bf.extents = slices.Insert(bf.extents, target, t)
		}
	} else {
		// split: curr.Loff < loff
		curr := bf.extents[target]
		so := uint32(loff - curr.Loff)
		left := bruteForceExtent{Loff: curr.Loff, Poff: curr.Poff, Len: so, Tag: curr.Tag}
		right := bruteForceExtent{Loff: loff, Poff: curr.Poff + uint64(so), Len: curr.Len - so, Tag: 0}
		bf.extents[target] = left
		bf.extents = slices.Insert(bf.extents, target+1, t, right)
		shift = 2
	}

	for i := target + shift; i < len(bf.extents); i++ {
		bf.extents[i].Loff += uint64(length)
	}
	bf.maxLoff_ += uint64(length)
	return nil
}

// InsertWithTag inserts an extent with an associated tag.
func (bf *bruteForce) InsertWithTag(loff, poff uint64, length uint32, tag uint16) error {
	return bf.insertR(loff, poff, length, tag)
}

// Delete removes a logical range, shifting subsequent extents left.
func (bf *bruteForce) Delete(loff, length uint64) error {
	if loff+length > bf.maxLoff_ {
		return ErrOutOfRange
	}
	olen := length
	for olen > 0 {
		target := bf.findPos(loff)
		curr := bf.extents[target]
		tlen := min(curr.Loff+uint64(curr.Len)-loff, olen)
		shift := 1
		if curr.Loff == loff {
			curr.Len -= uint32(tlen)
			curr.Poff += tlen
			curr.Tag = 0
			if curr.Len == 0 {
				bf.extents = slices.Delete(bf.extents, target, target+1)
				shift = 0
			} else {
				bf.extents[target] = curr
			}
		} else {
			tmp := uint32(loff - curr.Loff)
			if curr.Len-tmp == uint32(tlen) {
				curr.Len -= uint32(tlen)
				bf.extents[target] = curr
			} else {
				right := bruteForceExtent{
					Loff: loff + tlen,
					Poff: curr.Poff + uint64(tmp) + tlen,
					Len:  curr.Len - tmp - uint32(tlen),
					Tag:  0,
				}
				curr.Len = tmp
				bf.extents[target] = curr
				bf.extents = slices.Insert(bf.extents, target+1, right)
			}
		}
		for i := target + shift; i < len(bf.extents); i++ {
			bf.extents[i].Loff -= tlen
		}
		olen -= tlen
	}
	bf.maxLoff_ -= length
	return nil
}

// Query returns all extents covering the range [loff, loff+length).
func (bf *bruteForce) Query(loff, length uint64) []QueryResult {
	if loff+length > bf.maxLoff_ {
		return nil
	}
	oloff := loff
	olen := length
	target := bf.findPos(loff)
	var res []QueryResult
	for olen > 0 {
		if target >= len(bf.extents) {
			return nil
		}
		curr := &bf.extents[target]
		if curr.Loff > oloff || curr.Loff+uint64(curr.Len) <= oloff {
			return nil
		}
		tlen := min(curr.Loff+uint64(curr.Len)-oloff, olen)
		res = append(res, QueryResult{
			Poff: curr.Poff + (oloff - curr.Loff),
			Len:  tlen,
		})
		oloff += tlen
		olen -= tlen
		target++
	}
	return res
}

// SetTag sets a tag at a logical offset, splitting the extent if needed.
func (bf *bruteForce) SetTag(loff uint64, tag uint16) error {
	if loff >= bf.maxLoff_ {
		return ErrOutOfRange
	}
	target := bf.findPos(loff)
	if target >= len(bf.extents) {
		return ErrOutOfRange
	}
	curr := bf.extents[target]
	if curr.Loff == loff {
		bf.extents[target].Tag = tag
	} else {
		so := uint32(loff - curr.Loff)
		left := bruteForceExtent{Loff: curr.Loff, Poff: curr.Poff, Len: so, Tag: curr.Tag}
		right := bruteForceExtent{Loff: loff, Poff: curr.Poff + uint64(so), Len: curr.Len - so, Tag: tag}
		bf.extents[target] = left
		bf.extents = slices.Insert(bf.extents, target+1, right)
	}
	return nil
}

// GetTag retrieves the tag at a logical offset.
func (bf *bruteForce) GetTag(loff uint64) (uint16, error) {
	if loff >= bf.maxLoff_ {
		return 0, ErrOutOfRange
	}
	target := bf.findPos(loff)
	if target >= len(bf.extents) {
		return 0, ErrOutOfRange
	}
	curr := &bf.extents[target]
	if curr.Loff == loff && curr.Tag != 0 {
		return curr.Tag, nil
	}
	return 0, ErrTagNotFound
}

// UpdatePoff updates the physical offset of an existing extent.
func (bf *bruteForce) UpdatePoff(loff, poff uint64, length uint32) error {
	if loff+uint64(length) > bf.maxLoff_ {
		return ErrOutOfRange
	}
	target := bf.findPos(loff)
	if target >= len(bf.extents) {
		return errors.New("not found")
	}
	curr := &bf.extents[target]
	if curr.Loff != loff || curr.Len != length {
		return errors.New("mismatch")
	}
	curr.Poff = poff
	return nil
}
