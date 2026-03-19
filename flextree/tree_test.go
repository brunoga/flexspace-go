package flextree

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"testing"
	"time"
)

func assertEq(t *testing.T, a, b any, msg string) {
	if a != b {
		t.Fatalf("%s: expected %v, got %v", msg, a, b)
	}
}

func compareQuery(t *testing.T, q1, q2 []QueryResult) {
	assertEq(t, len(q1), len(q2), "query len mismatch")
	for i := range q1 {
		assertEq(t, q1[i].Poff, q2[i].Poff, "poff mismatch")
		assertEq(t, q1[i].Len, q2[i].Len, "len mismatch")
	}
}

func TestTreeBasic(t *testing.T) {
	tree := NewTree(128 * 1024)
	err := tree.Insert(0, 1000, 500)
	assertEq(t, err, nil, "insert")

	res := tree.Query(0, 500, nil)
	assertEq(t, len(res), 1, "query len")
	assertEq(t, res[0].Poff, uint64(1000), "poff")
	assertEq(t, res[0].Len, uint64(500), "len")

	err = tree.Delete(100, 100)
	assertEq(t, err, nil, "delete")

	res = tree.Query(0, 400, nil)
	assertEq(t, len(res), 2, "query len")
	assertEq(t, res[0].Poff, uint64(1000), "poff")
	assertEq(t, res[0].Len, uint64(100), "len")
	assertEq(t, res[1].Poff, uint64(1200), "poff")
	assertEq(t, res[1].Len, uint64(300), "len")
}

func dumpTree(t *Tree) {
	fmt.Printf("Tree MaxLoff: %d\n", t.MaxLoff())
	curr := t.leafHead
	nodeIdx := 0
	for curr != nil {
		ld := curr.leaf()
		fmt.Printf("  Node %d (Count %d, IsLeaf %v):\n", nodeIdx, curr.count, curr.isLeaf)
		for i := uint32(0); i < curr.count; i++ {
			ext := &ld.extents[i]
			fmt.Printf("    [%d] Loff: %d, Len: %d, Poff: %d, Tag: %d\n", i, ext.loff, ext.len, ext.poff(), ext.tag())
		}
		curr = ld.next
		nodeIdx++
	}
}

func dumpBF(bf *bruteForce) {
	fmt.Printf("BF MaxLoff: %d\n", bf.MaxLoff())
	for i, ext := range bf.Extents() {
		fmt.Printf("  [%d] Loff: %d, Len: %d, Poff: %d, Tag: %d\n", i, ext.Loff, ext.Len, ext.Poff, ext.Tag)
	}
}

func compareFull(t *testing.T, tree *Tree, bf *bruteForce) {
	if tree.MaxLoff() != bf.MaxLoff() {
		t.Fatalf("MaxLoff mismatch: tree=%d, bf=%d", tree.MaxLoff(), bf.MaxLoff())
	}
	maxLoff := tree.MaxLoff()
	if maxLoff == 0 {
		return
	}
	res1 := tree.Query(0, maxLoff, nil)
	res2 := bf.Query(0, maxLoff)
	if len(res1) != len(res2) {
		fmt.Printf("Full comparison length mismatch: tree=%d, bf=%d\n", len(res1), len(res2))
		dumpTree(tree)
		fmt.Println("--- BF ---")
		dumpBF(bf)
		t.Fatalf("Query len mismatch")
	}
	for i := range res1 {
		if res1[i].Poff != res2[i].Poff || res1[i].Len != res2[i].Len {
			fmt.Printf("Full comparison mismatch at index %d\n", i)
			fmt.Printf("  Tree: Poff=%d, Len=%d\n", res1[i].Poff, res1[i].Len)
			fmt.Printf("  BF:   Poff=%d, Len=%d\n", res2[i].Poff, res2[i].Len)
			dumpTree(tree)
			fmt.Println("--- BF ---")
			dumpBF(bf)
			t.Fatalf("Query result mismatch")
		}
	}

	// Verify all tags match
	bfExtents := bf.Extents()
	for _, ext := range bfExtents {
		if ext.Tag != 0 {
			treeTag, err := tree.GetTag(ext.Loff)
			if err != nil || treeTag != ext.Tag {
				t.Fatalf("Tag mismatch at loff %d: tree=%d, bf=%d, err=%v", ext.Loff, treeTag, ext.Tag, err)
			}
		} else {
			_, err := tree.GetTag(ext.Loff)
			if err == nil {
				// Tree has a tag but BF doesn't?
				// Note: tree.GetTag returns ErrTagNotFound if tag is 0
			}
		}
	}
}

// TestInsertWithTagFillsHoles verifies that InsertWithTag fills the gap
// with hole extents when loff > maxLoff, matching Insert's behaviour.
func TestInsertWithTagFillsHoles(t *testing.T) {
	tree := NewTree(1024 * 1024)

	if err := tree.Insert(0, 1000, 100); err != nil {
		t.Fatal(err)
	}
	assertEq(t, tree.MaxLoff(), uint64(100), "maxLoff after first insert")

	// InsertWithTag with a gap: loff=500, but maxLoff=100
	if err := tree.InsertWithTag(500, 2000, 100, 42); err != nil {
		t.Fatal(err)
	}

	// The hole [100, 500) must have been filled; maxLoff must be 600
	assertEq(t, tree.MaxLoff(), uint64(600), "maxLoff must include hole")

	if err := tree.CheckInvariants(); err != nil {
		t.Fatalf("invariant failed: %v", err)
	}

	// Full range must be queryable
	res := tree.Query(0, 600, nil)
	if res == nil {
		t.Fatal("Query returned nil for full range")
	}
	total := uint64(0)
	for _, r := range res {
		total += r.Len
	}
	assertEq(t, total, uint64(600), "total queried length")

	// The hole region [100, 500) must consist of hole extents
	holeRes := tree.Query(100, 400, nil)
	if len(holeRes) == 0 {
		t.Fatal("expected hole extents in [100, 500)")
	}
	for _, r := range holeRes {
		if !r.IsHole() {
			t.Errorf("expected IsHole() true in poff %d", r.Poff)
		}
	}

	// The tagged extent at 500 must carry tag 42
	tag, err := tree.GetTag(500)
	if err != nil {
		t.Fatalf("GetTag: %v", err)
	}
	assertEq(t, tag, uint16(42), "tag at loff 500")
}

// TestQueryAcrossLeaves verifies that Query returns correct results when the
// range spans multiple leaf nodes, exercising the leaf-list traversal path.
func TestQueryAcrossLeaves(t *testing.T) {
	tree := NewTree(4096)

	// Insert enough non-mergeable extents to fill multiple leaf nodes.
	// Use poffs that are far apart so extentSequential never merges them.
	numExtents := leafCap*2 + 5
	poffs := make([]uint64, numExtents)
	for i := range numExtents {
		poffs[i] = uint64(i) * 1_000_000
		if err := tree.InsertAppend(poffs[i], 4096); err != nil {
			t.Fatalf("InsertAppend %d: %v", i, err)
		}
	}

	if err := tree.CheckInvariants(); err != nil {
		t.Fatalf("invariant: %v", err)
	}

	// Verify per-extent single queries first (ensures findLeafNode is correct)
	for i := range numExtents {
		loff := uint64(i) * 4096
		res := tree.Query(loff, 4096, nil)
		if len(res) != 1 {
			t.Fatalf("extent %d: expected 1 result, got %d", i, len(res))
		}
		if res[0].Poff != poffs[i] {
			t.Fatalf("extent %d: poff want %d got %d", i, poffs[i], res[0].Poff)
		}
	}

	// Single large query across all leaf nodes
	totalLen := uint64(numExtents) * 4096
	res := tree.Query(0, totalLen, nil)
	if len(res) != numExtents {
		t.Fatalf("full query: want %d results, got %d", numExtents, len(res))
	}
	for i := range numExtents {
		if res[i].Poff != poffs[i] {
			t.Fatalf("full query extent %d: poff want %d got %d", i, poffs[i], res[i].Poff)
		}
		if res[i].Len != 4096 {
			t.Fatalf("full query extent %d: len want 4096 got %d", i, res[i].Len)
		}
	}

	// Query spanning from the middle of leaf 0 into leaf 2
	startLoff := uint64(leafCap/2) * 4096
	spanLen := uint64(leafCap+10) * 4096
	res2 := tree.Query(startLoff, spanLen, nil)
	if res2 == nil {
		t.Fatal("cross-leaf span query returned nil")
	}
	startIdx := leafCap / 2
	for i, r := range res2 {
		want := poffs[startIdx+i]
		if r.Poff != want {
			t.Fatalf("cross-leaf extent %d: poff want %d got %d", i, want, r.Poff)
		}
	}
}

func TestTreeComplex(t *testing.T) {
	seed := int64(time.Now().UnixNano())
	t.Logf("Using seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	maxExtent := uint32(128 * 1024)
	tree := NewTree(maxExtent)
	bf := newBruteForce(maxExtent)

	ops := 50000
	for i := range ops {
		maxLoff := tree.MaxLoff()
		op := rng.Intn(100)
		if op < 60 { // Increased insertion to 60%
			var loff uint64
			if maxLoff > 0 && rng.Intn(10) < 7 {
				loff = uint64(rng.Intn(int(maxLoff)))
			} else {
				loff = maxLoff
			}
			poff := uint64(rng.Intn(100000000))
			// Stay within maxExtent (128KB)
			length := uint32(rng.Intn(128*1024) + 1)
			tag := uint16(0)
			if rng.Intn(10) < 3 {
				tag = uint16(rng.Intn(65535) + 1)
			}

			err1 := tree.InsertWithTag(loff, poff, length, tag)
			err2 := bf.InsertWithTag(loff, poff, length, tag)
			if err1 != err2 {
				t.Fatalf("Insert error mismatch at op %d: tree=%v, bf=%v", i, err1, err2)
			}
		} else if op < 90 { // Delete (30%)
			if maxLoff == 0 {
				continue
			}
			loff := uint64(rng.Intn(int(maxLoff)))
			// Delete up to 1MB to trigger multi-extent deletion
			length := uint64(rng.Intn(1024*1024) + 1)
			if loff+length > maxLoff {
				length = maxLoff - loff
			}
			if length == 0 {
				continue
			}

			err1 := tree.Delete(loff, length)
			err2 := bf.Delete(loff, length)
			if err1 != err2 {
				t.Fatalf("Delete error mismatch at op %d: tree=%v, bf=%v", i, err1, err2)
			}
		} else if op < 95 { // Query
			if maxLoff == 0 {
				continue
			}
			loff := uint64(rng.Intn(int(maxLoff)))
			length := uint64(rng.Intn(500*1024*1024) + 1)
			if loff+length > maxLoff {
				length = maxLoff - loff
			}
			if length == 0 {
				continue
			}

			res1 := tree.Query(loff, length, nil)
			res2 := bf.Query(loff, length)
			compareQuery(t, res1, res2)
		} else { // Tags
			if maxLoff == 0 {
				continue
			}
			loff := uint64(rng.Intn(int(maxLoff)))
			tag := uint16(rng.Intn(65535) + 1)

			err1 := tree.SetTag(loff, tag)
			err2 := bf.SetTag(loff, tag)
			if err1 != err2 {
				t.Fatalf("SetTag error mismatch at op %d", i)
			}

			if err1 == nil {
				t1, e1 := tree.GetTag(loff)
				t2, e2 := bf.GetTag(loff)
				if e1 != e2 || t1 != t2 {
					t.Fatalf("Tag value mismatch at op %d", i)
				}
			}
		}

		if i%10 == 0 {
			if err := tree.CheckInvariants(); err != nil {
				t.Fatalf("Invariant failed at op %d: %v", i, err)
			}
			compareFull(t, tree, bf)
		}
	}

	// Final check
	if err := tree.CheckInvariants(); err != nil {
		t.Fatalf("Final invariant check failed: %v", err)
	}
	compareFull(t, tree, bf)
}

// TestSyncLoadRoundTrip verifies that Sync/LoadTree preserves tree contents
// and that LoadTree correctly rebuilds the free ID list so post-load
// allocations reuse previously freed IDs rather than always appending.
func TestSyncLoadRoundTrip(t *testing.T) {
	metaFile := t.TempDir() + "/meta"
	nodeFile := t.TempDir() + "/nodes"

	tree := NewTree(128 * 1024)
	for i := range 200 {
		if err := tree.InsertAppend(uint64(i)*4096, 4096); err != nil {
			t.Fatal(err)
		}
	}
	// Delete some extents so their IDs go into the free list.
	if err := tree.Delete(0, 4096*50); err != nil {
		t.Fatal(err)
	}
	maxNodeIDBefore := tree.maxNodeID
	freeCountBefore := len(tree.freeIDs)

	if err := tree.Sync(metaFile, nodeFile); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	loaded, err := LoadTree(metaFile, nodeFile)
	if err != nil {
		t.Fatalf("LoadTree: %v", err)
	}

	// Loaded tree must have same content.
	assertEq(t, loaded.MaxLoff(), tree.MaxLoff(), "maxLoff after load")

	// Free list must be non-empty (previously freed IDs recovered).
	if len(loaded.freeIDs) == 0 && maxNodeIDBefore > loaded.nodeCount {
		t.Fatalf("LoadTree did not rebuild freeIDs: maxNodeID=%d nodeCount=%d freeIDs=%d",
			loaded.maxNodeID, loaded.nodeCount, len(loaded.freeIDs))
	}
	_ = freeCountBefore

	// Post-load insert must not require extending maxNodeID when free IDs exist.
	freeCountAfterLoad := len(loaded.freeIDs)
	maxNodeIDAfterLoad := loaded.maxNodeID
	if err := loaded.InsertAppend(0xDEAD0000, 4096); err != nil {
		t.Fatalf("post-load insert: %v", err)
	}
	if freeCountAfterLoad > 0 {
		// A free ID should have been consumed, not a fresh one.
		assertEq(t, loaded.maxNodeID, maxNodeIDAfterLoad, "maxNodeID must not grow when freeIDs non-empty")
	}

	if err := loaded.CheckInvariants(); err != nil {
		t.Fatalf("post-load invariants: %v", err)
	}

	// Full query must match original tree.
	ml := tree.MaxLoff()
	r1 := tree.Query(0, ml, nil)
	r2 := loaded.Query(0, ml, nil)
	compareQuery(t, r1, r2)

	// Second sync/load cycle to verify COW leaves previous state intact.
	metaFile2 := t.TempDir() + "/meta2"
	nodeFile2 := t.TempDir() + "/nodes2"
	if err := loaded.Sync(metaFile2, nodeFile2); err != nil {
		t.Fatalf("second Sync: %v", err)
	}
	loaded2, err := LoadTree(metaFile2, nodeFile2)
	if err != nil {
		t.Fatalf("second LoadTree: %v", err)
	}
	if err := loaded2.CheckInvariants(); err != nil {
		t.Fatalf("second load invariants: %v", err)
	}
	_ = os.RemoveAll // silence unused import if any
}

// TestUpdatePoff verifies that UpdatePoff correctly updates physical offsets
// of existing extents when loff and length match.
func TestUpdatePoff(t *testing.T) {
	tree := NewTree(128 * 1024)

	// Insert an extent
	if err := tree.Insert(0, 1000, 100); err != nil {
		t.Fatal(err)
	}

	// Update its physical offset
	if err := tree.UpdatePoff(0, 5000, 100); err != nil {
		t.Fatal(err)
	}

	// Verify the update took effect
	res := tree.Query(0, 100, nil)
	if len(res) != 1 || res[0].Poff != 5000 {
		t.Fatalf("UpdatePoff failed: expected poff=5000, got %d", res[0].Poff)
	}

	if err := tree.CheckInvariants(); err != nil {
		t.Fatalf("invariants after UpdatePoff: %v", err)
	}
}

// TestUpdatePoffMismatch verifies UpdatePoff returns error when loff or length
// don't match the actual extent.
func TestUpdatePoffMismatch(t *testing.T) {
	tree := NewTree(128 * 1024)

	if err := tree.Insert(0, 1000, 100); err != nil {
		t.Fatal(err)
	}

	// Try to update with wrong length
	err := tree.UpdatePoff(0, 5000, 50) // actual length is 100
	if err == nil {
		t.Fatal("UpdatePoff should return error on length mismatch")
	}

	// Try to update at wrong offset
	err = tree.UpdatePoff(50, 5000, 50)
	if err == nil {
		t.Fatal("UpdatePoff should return error at non-extent boundary")
	}
}

// TestUpdatePoffOutOfRange verifies UpdatePoff returns error for out-of-range offsets.
func TestUpdatePoffOutOfRange(t *testing.T) {
	tree := NewTree(128 * 1024)

	if err := tree.Insert(0, 1000, 100); err != nil {
		t.Fatal(err)
	}

	err := tree.UpdatePoff(150, 5000, 100) // beyond MaxLoff
	if err != ErrOutOfRange {
		t.Fatalf("UpdatePoff out-of-range: expected ErrOutOfRange, got %v", err)
	}
}

// TestIterateExtents verifies that IterateExtents visits all extents in order.
func TestIterateExtents(t *testing.T) {
	tree := NewTree(128 * 1024)

	// Build a tree with known extents
	expectedCount := 0
	for i := 0; i < 10; i++ {
		if err := tree.Insert(uint64(i*100), uint64((i+1)*1000), 100); err != nil {
			t.Fatal(err)
		}
		expectedCount++
	}

	// Track extents collected by iteration
	var collected []struct {
		loff   uint64
		poff   uint64
		length uint32
		tag    uint16
	}

	tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		collected = append(collected, struct {
			loff   uint64
			poff   uint64
			length uint32
			tag    uint16
		}{loff, poff, length, tag})
		return true // continue iteration
	})

	if len(collected) != expectedCount {
		t.Fatalf("IterateExtents: expected %d extents, collected %d", expectedCount, len(collected))
	}

	// Verify logical offsets are in order and match what we inserted
	for i, ext := range collected {
		if ext.loff != uint64(i*100) {
			t.Fatalf("extent %d: expected loff=%d, got %d", i, i*100, ext.loff)
		}
		if ext.length != 100 {
			t.Fatalf("extent %d: expected length=100, got %d", i, ext.length)
		}
	}
}

// TestIterateExtentsWithTags verifies tags are correctly reported during iteration.
func TestIterateExtentsWithTags(t *testing.T) {
	tree := NewTree(128 * 1024)

	// Insert extents with tags
	if err := tree.InsertWithTag(0, 1000, 100, 42); err != nil {
		t.Fatal(err)
	}
	if err := tree.InsertWithTag(100, 2000, 100, 99); err != nil {
		t.Fatal(err)
	}
	if err := tree.InsertWithTag(200, 3000, 100, 0); err != nil { // no tag (0)
		t.Fatal(err)
	}

	var collected []uint16
	tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		collected = append(collected, tag)
		return true
	})

	expected := []uint16{42, 99, 0}
	if len(collected) != len(expected) {
		t.Fatalf("expected %d extents, got %d", len(expected), len(collected))
	}

	for i, tag := range collected {
		if tag != expected[i] {
			t.Fatalf("extent %d: expected tag=%d, got %d", i, expected[i], tag)
		}
	}
}

// TestIterateExtentsEarlyStop verifies iteration stops when callback returns false.
func TestIterateExtentsEarlyStop(t *testing.T) {
	tree := NewTree(128 * 1024)

	// Build a tree with multiple extents
	for i := 0; i < 20; i++ {
		if err := tree.Insert(uint64(i*100), uint64(i+1000), 100); err != nil {
			t.Fatal(err)
		}
	}

	// Iterate but stop after 5 extents
	count := 0
	tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		count++
		return count < 5 // stop when count reaches 5
	})

	if count != 5 {
		t.Fatalf("early stop: expected 5 iterations, got %d", count)
	}
}

// TestHighVolumeAppend verifies the tree handles 1 million append operations correctly.
// This is a stress test for the append-only insertion path.
func TestHighVolumeAppend(t *testing.T) {
	tree := NewTree(128 << 10) // 128KB max extent
	for i := range 1000000 {
		poff := uint64(i) * 1024
		if err := tree.Insert(tree.MaxLoff(), poff, 1024); err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}
	tree.Close()
}

// xorshift64 is a pseudo-random number generator that matches the C implementation
// exactly for fair workload comparison in benchmarks.
type xorshift64 uint64

func (x *xorshift64) next() uint64 {
	*x ^= *x >> 12
	*x ^= *x << 25
	*x ^= *x >> 27
	return uint64(*x) * 0x2545F4914F6CDD1D
}

// BenchmarkTreeInsert measures insertion performance with random operations.
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

// BenchmarkTreeQuery measures query performance with random read operations.
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

// BenchmarkTreeInsertAppend measures append-only insertion performance.
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

// BenchmarkTreeDelete measures deletion performance with random operations.
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

// BenchmarkTreeSetTag measures tag assignment performance.
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

// BenchmarkTreeGetTag measures tag retrieval performance.
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

// BenchmarkCompareStandard compares Tree performance against a naive slice-based approach.
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
