package flexfile

import (
	"bytes"
	"testing"
)

func FuzzFlexFile(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 10 {
			return
		}

		path := t.TempDir()
		ff, err := Open(path)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer ff.Close()

		// Reference model: simple byte slice
		var model []byte

		off := 0
		for off+8 < len(data) {
			op := data[off] % 6
			off++

			switch op {
			case 0: // Insert
				pos := uint64(binaryUint32(data, off)) % uint64(len(model)+1)
				length := int(data[off+4])%1024 + 1
				val := bytes.Repeat([]byte{data[off+5]}, length)
				off += 6

				n, err := ff.Insert(val, pos)
				if err != nil {
					t.Fatalf("Insert: %v", err)
				}
				if n != length {
					t.Fatalf("Insert: n=%d, want %d", n, length)
				}
				// Update model
				newModel := make([]byte, len(model)+length)
				copy(newModel[:pos], model[:pos])
				copy(newModel[pos:pos+uint64(length)], val)
				copy(newModel[pos+uint64(length):], model[pos:])
				model = newModel

			case 1: // Delete (Collapse)
				if len(model) == 0 {
					continue
				}
				pos := uint64(binaryUint32(data, off)) % uint64(len(model))
				length := uint64(data[off+4])%uint64(len(model)-int(pos)) + 1
				off += 5

				err := ff.Collapse(pos, length)
				if err != nil {
					t.Fatalf("Collapse: %v", err)
				}
				// Update model
				model = append(model[:pos], model[pos+length:]...)

			case 2: // Write
				if len(model) == 0 {
					continue
				}
				pos := uint64(binaryUint32(data, off)) % uint64(len(model))
				length := int(data[off+4])%1024 + 1
				if int(pos)+length > len(model) {
					length = len(model) - int(pos)
				}
				val := bytes.Repeat([]byte{data[off+5]}, length)
				off += 6

				n, err := ff.Update(val, pos, uint64(length))
				if err != nil {
					t.Fatalf("Write: %v", err)
				}
				if n != length {
					t.Fatalf("Write: n=%d, want %d", n, length)
				}
				copy(model[pos:pos+uint64(length)], val)

			case 3: // Update
				if len(model) == 0 {
					continue
				}
				pos := uint64(binaryUint32(data, off)) % uint64(len(model))
				olen := uint64(data[off+4])%uint64(len(model)-int(pos)) + 1
				nlen := int(data[off+5])%1024 + 1
				val := bytes.Repeat([]byte{data[off+6]}, nlen)
				off += 7

				n, err := ff.Update(val, pos, olen)
				if err != nil {
					t.Fatalf("Update: %v", err)
				}
				if n != nlen {
					t.Fatalf("Update: n=%d, want %d", n, nlen)
				}
				// Update model
				newModel := make([]byte, len(model)-int(olen)+nlen)
				copy(newModel[:pos], model[:pos])
				copy(newModel[pos:pos+uint64(nlen)], val)
				copy(newModel[pos+uint64(nlen):], model[pos+olen:])
				model = newModel

			case 4: // SetTag
				if len(model) == 0 {
					continue
				}
				pos := uint64(binaryUint32(data, off)) % uint64(len(model))
				tag := uint16(binaryUint32(data, off+4))
				off += 8
				ff.SetTag(pos, tag)

			case 5: // Sync
				_ = ff.Sync()
			}

			// Verify data consistency
			if len(model) > 0 {
				readBuf := make([]byte, len(model))
				n, err := ff.Read(readBuf, 0)
				if err != nil {
					t.Fatalf("Read: %v", err)
				}
				if n != len(model) {
					t.Fatalf("Read: n=%d, want %d", n, len(model))
				}
				if !bytes.Equal(readBuf, model) {
					t.Fatalf("Data mismatch at op %d", op)
				}
			}
		}
	})
}

func binaryUint32(data []byte, off int) uint32 {
	var v uint32
	for i := 0; i < 4 && off+i < len(data); i++ {
		v |= uint32(data[off+i]) << (i * 8)
	}
	return v
}
