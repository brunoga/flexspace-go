package flextree

import (
	"testing"
)

func TestHighVolumeAppend(t *testing.T) {
	tree := NewTree(128 << 10) // 128KB max extent
	for i := range 1000000 {
		poff := uint64(i) * 1024
		if err := tree.Insert(tree.MaxLoff(), poff, 1024); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}
}
