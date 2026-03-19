package flextree

import (
	"fmt"
	"slices"
	"testing"
)

// xorshift64 matches the C implementation exactly for fair workload comparison.
type xorshift64 uint64

func (x *xorshift64) next() uint64 {
	*x ^= *x >> 12
	*x ^= *x << 25
	*x ^= *x >> 27
	return uint64(*x) * 0x2545F4914F6CDD1D
}

func BenchmarkTreeInsert(b *testing.B) {
	state := xorshift64(42)

	type op struct {
		loff uint64
		poff uint64
		len  uint32
	}

	const numOps = 1000000
	ops := make([]op, numOps)
	maxLoff := uint64(0)

	for i := range numOps {
		var loff uint64
		if maxLoff > 0 && (state.next()%10) < 5 {
			loff = state.next() % maxLoff
		} else {
			loff = maxLoff
		}
		length := uint32((state.next() % 4096) + 1)
		ops[i] = op{
			loff: loff,
			poff: state.next() % (1 << 40),
			len:  length,
		}
		if loff+uint64(length) > maxLoff {
			maxLoff = loff + uint64(length)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewTree(128 * 1024)
		b.StartTimer()

		for j := 0; j < numOps && i < b.N; j++ {
			o := &ops[j]
			tree.Insert(o.loff, o.poff, o.len)
			i++
		}
		i--
		tree.Close()
	}
}

func BenchmarkTreeQuery(b *testing.B) {
	const numOps = 1000000
	state := xorshift64(42)

	type op struct {
		loff uint64
		poff uint64
		len  uint32
	}

	ops := make([]op, numOps)
	maxLoff := uint64(0)

	for i := range numOps {
		var loff uint64
		if maxLoff > 0 && (state.next()%10) < 5 {
			loff = state.next() % maxLoff
		} else {
			loff = maxLoff
		}
		length := uint32((state.next() % 4096) + 1)
		ops[i] = op{
			loff: loff,
			poff: state.next() % (1 << 40),
			len:  length,
		}
		if loff+uint64(length) > maxLoff {
			maxLoff = loff + uint64(length)
		}
	}

	tree := NewTree(128 * 1024)
	for i := range numOps {
		tree.Insert(ops[i].loff, ops[i].poff, ops[i].len)
	}

	stateQ := xorshift64(4242)
	qOffs := make([]uint64, numOps)
	for i := range numOps {
		qOffs[i] = stateQ.next() % maxLoff
	}

	b.ResetTimer()
	b.ReportAllocs()

	var buf [16]QueryResult
	for i := 0; i < b.N; i++ {
		tree.Query(qOffs[i%numOps], 1024, buf[:])
	}
	tree.Close()
}

func BenchmarkTreeInsertAppend(b *testing.B) {
	const numOps = 1_000_000
	state := xorshift64(42)

	type op struct {
		poff uint64
		len  uint32
	}
	ops := make([]op, numOps)
	for i := range numOps {
		ops[i] = op{
			poff: state.next() % (1 << 40),
			len:  uint32(state.next()%4096) + 1,
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewTree(128 * 1024)
		b.StartTimer()
		for j := 0; j < numOps && i < b.N; j++ {
			tree.InsertAppend(ops[j].poff, ops[j].len)
			i++
		}
		i--
		tree.Close()
	}
}

func BenchmarkTreeDelete(b *testing.B) {
	const numOps = 1_000_000

	// Build deterministic append-only fill to get a large tree.
	fillState := xorshift64(99)
	type fillOp struct {
		poff uint64
		len  uint32
	}
	fills := make([]fillOp, numOps)
	var maxLoff uint64
	for i := range numOps {
		fills[i] = fillOp{
			poff: fillState.next() % (1 << 40),
			len:  uint32(fillState.next()%4096) + 1,
		}
		maxLoff += uint64(fills[i].len)
	}

	// Pre-generate delete positions against the initial maxLoff.
	delState := xorshift64(4242)
	delOffs := make([]uint64, numOps)
	for i := range numOps {
		delOffs[i] = delState.next() % maxLoff
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tree := NewTree(128 * 1024)
		for _, f := range fills {
			tree.InsertAppend(f.poff, f.len)
		}
		curMax := maxLoff
		b.StartTimer()
		for j := 0; j < numOps && i < b.N; j++ {
			if curMax > 64 {
				tree.Delete(delOffs[j]%(curMax-64), 64)
				curMax -= 64
			}
			i++
		}
		i--
		tree.Close()
	}
}

func BenchmarkTreeSetTag(b *testing.B) {
	const numOps = 1_000_000
	state := xorshift64(42)

	// Build tree with appends, recording each extent's starting loff.
	tree := NewTree(128 * 1024)
	loffs := make([]uint64, numOps)
	var cur uint64
	for i := range numOps {
		loffs[i] = cur
		l := uint32(state.next()%4096) + 1
		tree.InsertAppend(state.next()%(1<<40), l)
		cur += uint64(l)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tree.SetTag(loffs[i%numOps], uint16(i))
	}
	tree.Close()
}

func BenchmarkTreeGetTag(b *testing.B) {
	const numOps = 1_000_000
	state := xorshift64(42)

	tree := NewTree(128 * 1024)
	loffs := make([]uint64, numOps)
	var cur uint64
	for i := range numOps {
		loffs[i] = cur
		l := uint32(state.next()%4096) + 1
		tree.InsertAppend(state.next()%(1<<40), l)
		cur += uint64(l)
	}
	// Pre-set all tags so GetTag always succeeds.
	for _, loff := range loffs {
		tree.SetTag(loff, 0xBEEF)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tree.GetTag(loffs[i%numOps])
	}
	tree.Close()
}

func BenchmarkCompareStandard(b *testing.B) {
	sizes := []int{1000, 10000, 100000}

	for _, size := range sizes {
		state := xorshift64(42)
		type op struct {
			loff uint64
			poff uint64
			len  uint32
		}
		ops := make([]op, size)
		maxLoff := uint64(0)
		for i := range size {
			var loff uint64
			if maxLoff > 0 && (state.next()%10) < 5 {
				loff = state.next() % maxLoff
			} else {
				loff = maxLoff
			}
			length := uint32((state.next() % 4096) + 1)
			ops[i] = op{loff: loff, poff: state.next() % (1 << 40), len: length}
			if loff+uint64(length) > maxLoff {
				maxLoff = loff + uint64(length)
			}
		}

		b.Run(fmt.Sprintf("FlexTree-Insert-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tree := NewTree(128 * 1024)
				b.StartTimer()
				for j := range size {
					tree.Insert(ops[j].loff, ops[j].poff, ops[j].len)
				}
				b.StopTimer()
				tree.Close()
				b.StartTimer()
			}
		})

		b.Run(fmt.Sprintf("Slice-Insert-%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var s []extent
				for j := range size {
					o := ops[j]
					// Find position
					idx, found := slices.BinarySearchFunc(s, o.loff, func(e extent, loff uint64) int {
						if uint64(e.loff) <= loff {
							if uint64(e.loff+e.len) > loff {
								return 0
							}
							return -1
						}
						return 1
					})

					if found {
						// This is a simplified "shifting insert" to match flextree behavior
						e := &s[idx]
						if uint64(e.loff) == o.loff {
							s = slices.Insert(s, idx, extent{loff: uint32(o.loff), len: o.len, pTag: o.poff})
						} else {
							// Split
							oldLen := e.len
							so := uint32(o.loff - uint64(e.loff))
							e.len = so
							right := extent{loff: uint32(o.loff) + o.len, len: oldLen - so, pTag: (e.pTag & pOffMask) + uint64(so)}
							s = slices.Insert(s, idx+1, extent{loff: uint32(o.loff), len: o.len, pTag: o.poff}, right)
						}
						for k := idx + 1; k < len(s); k++ {
							if k == idx+1 && s[k].loff == uint32(o.loff) {
								continue
							}
							s[k].loff += o.len
						}
					} else {
						// Append or regular insert
						s = slices.Insert(s, idx, extent{loff: uint32(o.loff), len: o.len, pTag: o.poff})
						for k := idx + 1; k < len(s); k++ {
							s[k].loff += o.len
						}
					}
				}
			}
		})
	}
}
