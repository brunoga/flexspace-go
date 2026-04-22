package flextree

import (
	"testing"
)

// TestBruteForceBasics validates fundamental bruteForce operations match expected behavior.
func TestBruteForceBasics(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Initially empty
	if bf.MaxLoff() != 0 {
		t.Fatalf("empty bf: MaxLoff should be 0, got %d", bf.MaxLoff())
	}
	if len(bf.Extents()) != 0 {
		t.Fatalf("empty bf: Extents should be empty")
	}

	// Insert first extent
	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if bf.MaxLoff() != 100 {
		t.Fatalf("after insert: MaxLoff should be 100, got %d", bf.MaxLoff())
	}
	if len(bf.Extents()) != 1 {
		t.Fatalf("after insert: should have 1 extent")
	}
}

// TestBruteForceInsertBasic tests single and sequential inserts.
func TestBruteForceInsertBasic(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Sequential append inserts should merge
	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	if err := bf.InsertWithTag(100, 1100, 100, 0); err != nil {
		t.Fatalf("insert 2: %v", err)
	}

	extents := bf.Extents()
	if len(extents) != 1 {
		t.Fatalf("sequential appends should merge: got %d extents", len(extents))
	}
	if extents[0].Len != 200 {
		t.Fatalf("merged extent: expected len 200, got %d", extents[0].Len)
	}
}

// TestBruteForceInsertWithGap tests inserts with gaps, which fill with hole extents.
func TestBruteForceInsertWithGap(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Insert at 0
	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatalf("insert 1: %v", err)
	}

	// Insert with gap: gap should be filled with hole
	if err := bf.InsertWithTag(500, 2000, 100, 0); err != nil {
		t.Fatalf("insert with gap: %v", err)
	}

	if bf.MaxLoff() != 600 {
		t.Fatalf("MaxLoff after gap insert: expected 600, got %d", bf.MaxLoff())
	}

	extents := bf.Extents()
	if len(extents) != 3 {
		t.Fatalf("gap insert: expected 3 extents (first, hole, third), got %d", len(extents))
	}

	// Check hole extent
	if extents[1].Poff&(1<<47) == 0 {
		t.Fatalf("middle extent should be a hole (holeBit set)")
	}
	if extents[1].Loff != 100 || extents[1].Len != 400 {
		t.Fatalf("hole: expected Loff=100 Len=400, got Loff=%d Len=%d", extents[1].Loff, extents[1].Len)
	}
}

// TestBruteForceInsertTooLarge tests error on extent size exceeding maxExtentSize.
func TestBruteForceInsertTooLarge(t *testing.T) {
	bf := newBruteForce(1024) // small limit

	err := bf.InsertWithTag(0, 1000, 2000, 0) // exceeds limit
	if err != ErrExtentTooLarge {
		t.Fatalf("oversized extent: expected ErrExtentTooLarge, got %v", err)
	}
}

// TestBruteForceQuery tests range queries.
func TestBruteForceQuery(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Build: [0, 100) -> poff 1000, [100, 200) -> poff 2000
	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}
	if err := bf.InsertWithTag(100, 2000, 100, 0); err != nil {
		t.Fatal(err)
	}

	// Query entire range
	res := bf.Query(0, 200)
	if len(res) != 2 {
		t.Fatalf("full query: expected 2 results, got %d", len(res))
	}
	if res[0].Poff != 1000 || res[0].Len != 100 {
		t.Fatalf("query [0]: expected Poff=1000 Len=100, got %d %d", res[0].Poff, res[0].Len)
	}
	if res[1].Poff != 2000 || res[1].Len != 100 {
		t.Fatalf("query [1]: expected Poff=2000 Len=100, got %d %d", res[1].Poff, res[1].Len)
	}

	// Partial query
	res = bf.Query(50, 100)
	if len(res) != 2 {
		t.Fatalf("partial query: expected 2 results")
	}
	if res[0].Len != 50 || res[1].Len != 50 {
		t.Fatalf("partial query slicing: expected 50+50, got %d+%d", res[0].Len, res[1].Len)
	}
}

// TestBruteForceQueryOutOfRange tests query boundary checks.
func TestBruteForceQueryOutOfRange(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	// Query beyond MaxLoff
	res := bf.Query(0, 150) // MaxLoff is 100
	if res != nil {
		t.Fatalf("out-of-range query: expected nil, got %v", res)
	}
}

// TestBruteForceDelete tests logical range deletion with shift left.
func TestBruteForceDelete(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Build: [0, 100), [100, 200), [200, 300)
	for i := 0; i < 3; i++ {
		poff := uint64((i + 1) * 1000)
		if err := bf.InsertWithTag(uint64(i*100), poff, 100, 0); err != nil {
			t.Fatal(err)
		}
	}

	// Delete middle extent
	if err := bf.Delete(100, 100); err != nil {
		t.Fatal(err)
	}

	if bf.MaxLoff() != 200 {
		t.Fatalf("after delete: MaxLoff expected 200, got %d", bf.MaxLoff())
	}

	extents := bf.Extents()
	if len(extents) != 2 {
		t.Fatalf("after delete: expected 2 extents, got %d", len(extents))
	}

	// Third extent should have shifted left
	if extents[1].Loff != 100 {
		t.Fatalf("third extent: expected Loff=100 after shift, got %d", extents[1].Loff)
	}
}

// TestBruteForceDeletePartial tests partial deletion splitting an extent.
func TestBruteForceDeletePartial(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Single large extent
	if err := bf.InsertWithTag(0, 1000, 1000, 0); err != nil {
		t.Fatal(err)
	}

	// Delete from middle: [250, 750)
	// This leaves [0, 250) and [750, 1000) which shifts to [250, 500)
	if err := bf.Delete(250, 500); err != nil {
		t.Fatal(err)
	}

	if bf.MaxLoff() != 500 {
		t.Fatalf("MaxLoff: expected 500, got %d", bf.MaxLoff())
	}

	extents := bf.Extents()
	if len(extents) != 2 {
		t.Fatalf("after partial delete: expected 2 extents, got %d", len(extents))
	}

	// Left part [0, 250)
	if extents[0].Loff != 0 || extents[0].Len != 250 {
		t.Fatalf("left extent: expected [0, 250), got [%d, %d)", extents[0].Loff, extents[0].Loff+uint64(extents[0].Len))
	}
	if extents[0].Poff != 1000 {
		t.Fatalf("left extent poff: expected 1000, got %d", extents[0].Poff)
	}

	// Right part [250, 500) which came from [750, 1000)
	if extents[1].Loff != 250 || extents[1].Len != 250 {
		t.Fatalf("right extent: expected [250, 500), got [%d, %d)", extents[1].Loff, extents[1].Loff+uint64(extents[1].Len))
	}
	if extents[1].Poff != 1750 { // 1000 + 750
		t.Fatalf("right extent poff: expected 1750, got %d", extents[1].Poff)
	}
}

// TestBruteForceDeleteOutOfRange tests error on out-of-range deletes.
func TestBruteForceDeleteOutOfRange(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	// Try to delete beyond MaxLoff
	err := bf.Delete(0, 150)
	if err != ErrOutOfRange {
		t.Fatalf("out-of-range delete: expected ErrOutOfRange, got %v", err)
	}
}

// TestBruteForceSetTag tests tag assignment and splitting.
func TestBruteForceSetTag(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	// Set tag at loff 0 (start): should not split
	if err := bf.SetTag(0, 42); err != nil {
		t.Fatal(err)
	}

	tag, err := bf.GetTag(0)
	if err != nil || tag != 42 {
		t.Fatalf("GetTag at 0: expected 42, got %d err=%v", tag, err)
	}

	// Set tag at middle: should split
	if err := bf.SetTag(50, 99); err != nil {
		t.Fatal(err)
	}

	extents := bf.Extents()
	if len(extents) != 2 {
		t.Fatalf("after mid-split: expected 2 extents, got %d", len(extents))
	}

	tag, err = bf.GetTag(50)
	if err != nil || tag != 99 {
		t.Fatalf("GetTag at 50: expected 99, got %d err=%v", tag, err)
	}
}

// TestBruteForceSetTagOutOfRange tests error on out-of-range SetTag.
func TestBruteForceSetTagOutOfRange(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	err := bf.SetTag(150, 42) // beyond MaxLoff
	if err != ErrOutOfRange {
		t.Fatalf("out-of-range SetTag: expected ErrOutOfRange, got %v", err)
	}
}

// TestBruteForceGetTag tests tag retrieval.
func TestBruteForceGetTag(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Insert with tag
	if err := bf.InsertWithTag(0, 1000, 100, 42); err != nil {
		t.Fatal(err)
	}

	// Tag exists at logical start
	tag, err := bf.GetTag(0)
	if err != nil || tag != 42 {
		t.Fatalf("GetTag at 0: expected 42, got %d err=%v", tag, err)
	}

	// No tag in middle (default 0 means no tag)
	tag, err = bf.GetTag(50)
	if err != ErrTagNotFound {
		t.Fatalf("GetTag in middle: expected ErrTagNotFound, got tag=%d err=%v", tag, err)
	}
}

// TestBruteForceGetTagOutOfRange tests error on out-of-range GetTag.
func TestBruteForceGetTagOutOfRange(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	_, err := bf.GetTag(150) // beyond MaxLoff
	if err != ErrOutOfRange {
		t.Fatalf("out-of-range GetTag: expected ErrOutOfRange, got %v", err)
	}
}

// TestBruteForceMultipleOperations tests a sequence of mixed operations.
func TestBruteForceMultipleOperations(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Insert several extents
	if err := bf.InsertWithTag(0, 1000, 100, 1); err != nil {
		t.Fatal(err)
	}
	if err := bf.InsertWithTag(100, 2000, 100, 2); err != nil {
		t.Fatal(err)
	}
	if err := bf.InsertWithTag(200, 3000, 100, 3); err != nil {
		t.Fatal(err)
	}

	if bf.MaxLoff() != 300 {
		t.Fatalf("MaxLoff: expected 300, got %d", bf.MaxLoff())
	}

	// Query
	res := bf.Query(0, 300)
	if len(res) != 3 {
		t.Fatalf("query: expected 3 results")
	}

	// Delete middle
	if err := bf.Delete(100, 100); err != nil {
		t.Fatal(err)
	}

	res = bf.Query(0, 200)
	if len(res) != 2 {
		t.Fatalf("after delete: expected 2 results, got %d", len(res))
	}

	// Verify tags
	tag, _ := bf.GetTag(0)
	if tag != 1 {
		t.Fatalf("tag at 0: expected 1, got %d", tag)
	}

	// Tag at former [200, 300) which is now at [100, 200)
	tag, _ = bf.GetTag(100)
	if tag != 3 {
		t.Fatalf("tag at 100 (shifted): expected 3, got %d", tag)
	}
}

// TestBruteForceExtents tests Extents() returns a proper copy.
func TestBruteForceExtents(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	ext1 := bf.Extents()
	ext2 := bf.Extents()

	if len(ext1) != len(ext2) {
		t.Fatalf("Extents() consistency")
	}
	if len(ext1) != 1 {
		t.Fatalf("expected 1 extent")
	}

	// Verify it's a copy (modifying the returned slice doesn't affect bf)
	ext1[0].Len = 999
	ext3 := bf.Extents()
	if ext3[0].Len != 100 {
		t.Fatalf("Extents() should return a copy; mutation leaked")
	}
}

// TestBruteForceClose tests Close() is a valid no-op.
func TestBruteForceClose(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	bf.Close() // should not panic or error

	// After Close, should still be functional (no-op)
	if bf.MaxLoff() != 100 {
		t.Fatalf("after Close: MaxLoff should still be 100")
	}
}

// TestBruteForceInsertZeroLength tests that zero-length inserts are no-ops.
func TestBruteForceInsertZeroLength(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	if err := bf.InsertWithTag(0, 1000, 100, 0); err != nil {
		t.Fatal(err)
	}

	// Zero-length insert should be no-op
	if err := bf.InsertWithTag(50, 5000, 0, 0); err != nil {
		t.Fatal(err)
	}

	if bf.MaxLoff() != 100 {
		t.Fatalf("after zero-length insert: MaxLoff should still be 100, got %d", bf.MaxLoff())
	}

	extents := bf.Extents()
	if len(extents) != 1 {
		t.Fatalf("zero-length insert should not create extents")
	}
}

// TestBruteForceInsertSplit tests insertion into the middle of an extent.
func TestBruteForceInsertSplit(t *testing.T) {
	bf := newBruteForce(128 * 1024)

	// Single extent [0, 1000)
	if err := bf.InsertWithTag(0, 5000, 1000, 0); err != nil {
		t.Fatal(err)
	}

	// Insert into middle: [300, 400)
	// This splits [0, 1000) into:
	//   [0, 300) + [300, 400) + [400, 1100) (shifted by 100)
	if err := bf.InsertWithTag(300, 6000, 100, 42); err != nil {
		t.Fatal(err)
	}

	extents := bf.Extents()
	if len(extents) != 3 {
		t.Fatalf("split insert: expected 3 extents (left, middle, right), got %d", len(extents))
	}

	// Verify structure
	if extents[0].Loff != 0 || extents[0].Len != 300 {
		t.Fatalf("left: expected [0, 300) len 300, got [%d, %d) len %d",
			extents[0].Loff, extents[0].Loff+uint64(extents[0].Len), extents[0].Len)
	}

	if extents[1].Loff != 300 || extents[1].Len != 100 || extents[1].Tag != 42 {
		t.Fatalf("middle: expected [300, 400) len 100 tag 42, got [%d, %d) len %d tag %d",
			extents[1].Loff, extents[1].Loff+uint64(extents[1].Len), extents[1].Len, extents[1].Tag)
	}

	// Right extent starts at 400 (after insert), length is 700 (1000 - 300 = 700)
	if extents[2].Loff != 400 || extents[2].Len != 700 {
		t.Fatalf("right: expected [400, 1100) len 700, got [%d, %d) len %d",
			extents[2].Loff, extents[2].Loff+uint64(extents[2].Len), extents[2].Len)
	}

	// Right extent poff should be 5000 + 300 = 5300
	if extents[2].Poff != 5300 {
		t.Fatalf("right poff: expected 5300, got %d", extents[2].Poff)
	}
}
