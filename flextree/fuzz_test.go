package flextree

import (
	"testing"
)

func FuzzTree(f *testing.F) {
	// Add some seed values
	f.Add([]byte{0, 10, 20, 30, 40})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 5 {
			return
		}

		maxExtent := uint32(128 * 1024)
		tree := NewTree(maxExtent)
		bf := newBruteForce(maxExtent)

		off := 0
		for off+4 < len(data) {
			op := data[off] % 4
			off++

			switch op {
			case 0: // Insert
				loff := uint64(data[off]) % (tree.MaxLoff() + 1)
				poff := uint64(binaryUint64(data, off+1)) % (1 << 40)
				length := uint32(data[off+2])%maxExtent + 1
				tag := uint16(data[off+3])
				off += 4

				err1 := tree.InsertWithTag(loff, poff, length, tag)
				err2 := bf.InsertWithTag(loff, poff, length, tag)
				if err1 != err2 {
					t.Fatalf("Insert error mismatch: tree=%v, bf=%v", err1, err2)
				}
			case 1: // Delete
				if tree.MaxLoff() == 0 {
					continue
				}
				loff := uint64(data[off]) % tree.MaxLoff()
				length := uint64(data[off+1]) % (tree.MaxLoff() - loff + 1)
				off += 2

				err1 := tree.Delete(loff, length)
				err2 := bf.Delete(loff, length)
				if err1 != err2 {
					t.Fatalf("Delete error mismatch: tree=%v, bf=%v", err1, err2)
				}
			case 2: // SetTag
				if tree.MaxLoff() == 0 {
					continue
				}
				loff := uint64(data[off]) % tree.MaxLoff()
				tag := uint16(data[off+1])
				off += 2

				err1 := tree.SetTag(loff, tag)
				err2 := bf.SetTag(loff, tag)
				if err1 != err2 {
					t.Fatalf("SetTag error mismatch")
				}
			case 3: // UpdatePoff
				if tree.MaxLoff() == 0 {
					continue
				}
				// Find an existing extent to update
				exts := bf.Extents()
				if len(exts) == 0 {
					continue
				}
				idx := int(data[off]) % len(exts)
				ext := exts[idx]
				newPoff := uint64(binaryUint64(data, off+1)) % (1 << 40)
				off += 4

				err1 := tree.UpdatePoff(ext.Loff, newPoff, ext.Len)
				err2 := bf.UpdatePoff(ext.Loff, newPoff, ext.Len)
				if err1 != err2 {
					t.Fatalf("UpdatePoff error mismatch")
				}
			}

			if err := tree.CheckInvariants(); err != nil {
				t.Fatalf("Invariant failed: %v", err)
			}
			compareFull(t, tree, bf)
		}
	})
}

func binaryUint64(data []byte, off int) uint64 {
	var v uint64
	for i := 0; i < 8 && off+i < len(data); i++ {
		v |= uint64(data[off+i]) << (i * 8)
	}
	return v
}
