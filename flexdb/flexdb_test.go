package flexdb

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "flexdb-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func mustOpenDB(t *testing.T, dir string) *DB {
	t.Helper()
	opts := DefaultOptions()
	opts.CacheMB = 64
	db, err := Open(context.Background(), dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return db
}

func mustTable(t *testing.T, db *DB, name string) *Table {
	t.Helper()
	tbl, err := db.Table(context.Background(), name)
	if err != nil {
		t.Fatalf("Table(%q): %v", name, err)
	}
	return tbl
}

func TestOpenClose(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPutGet(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	for i := range 100 {
		key := fmt.Appendf(nil, "key%05d", i)
		val := fmt.Appendf(nil, "val%05d", i)
		if err := ref.Put(context.Background(), key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := range 100 {
		key := fmt.Appendf(nil, "key%05d", i)
		want := string(fmt.Appendf(nil, "val%05d", i))
		got, err := ref.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("Get %d: got %q, want %q", i, got, want)
		}
	}

	got, err := ref.Get(context.Background(), []byte("nope"))
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("Get nope: expected nil, got %q", got)
	}
}

func TestProbe(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	key := []byte("hello")
	val := []byte("world")

	found, err := ref.Probe(context.Background(), key)
	if err != nil || found {
		t.Fatalf("Probe before put: found=%v err=%v", found, err)
	}
	if err := ref.Put(context.Background(), key, val); err != nil {
		t.Fatal(err)
	}
	found, err = ref.Probe(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("Probe after put: found=%v err=%v", found, err)
	}
}

func TestDelete(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	key := []byte("delme")
	val := []byte("value")

	if err := ref.Put(context.Background(), key, val); err != nil {
		t.Fatal(err)
	}
	if err := ref.Delete(context.Background(), key); err != nil {
		t.Fatal(err)
	}

	got, err := ref.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("expected nil after delete, got %q", got)
	}
}

func TestIterator(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	n := 50
	for i := range n {
		key := fmt.Appendf(nil, "k%05d", i)
		val := fmt.Appendf(nil, "v%05d", i)
		if err := ref.Put(context.Background(), key, val); err != nil {
			t.Fatal(err)
		}
	}

	it := ref.NewIterator()
	it.Seek([]byte("k00000"))
	defer it.Close()

	count := 0
	for it.Valid() {
		kv := it.Current()
		want := string(fmt.Appendf(nil, "k%05d", count))
		if string(kv.Key) != want {
			t.Fatalf("iter[%d]: got key %q, want %q", count, kv.Key, want)
		}
		count++
		it.Next()
	}
	if count != n {
		t.Fatalf("iterator returned %d entries, want %d", count, n)
	}
}

func TestSyncAndReopen(t *testing.T) {
	dir := tempDir(t)
	{
		db := mustOpenDB(t, dir)
		tbl := mustTable(t, db, "test")
		ref := tbl.NewRef()
		for i := range 20 {
			key := fmt.Appendf(nil, "key%05d", i)
			val := fmt.Appendf(nil, "val%05d", i)
			if err := ref.Put(context.Background(), key, val); err != nil {
				t.Fatal(err)
			}
		}
		ref.Sync(context.Background())
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	{
		db := mustOpenDB(t, dir)
		defer db.Close()
		tbl := mustTable(t, db, "test")
		ref := tbl.NewRef()
		for i := range 20 {
			key := fmt.Appendf(nil, "key%05d", i)
			want := string(fmt.Appendf(nil, "val%05d", i))
			got, err := ref.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("reopen Get %d: %v", i, err)
			}
			if string(got) != want {
				t.Fatalf("reopen Get %d: got %q, want %q", i, got, want)
			}
		}
	}
}

func TestLargeDataset(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	n := 500
	for i := range n {
		key := fmt.Appendf(nil, "key%08d", i)
		val := fmt.Appendf(nil, "val%08d", i)
		if err := ref.Put(context.Background(), key, val); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := range n {
		key := fmt.Appendf(nil, "key%08d", i)
		want := string(fmt.Appendf(nil, "val%08d", i))
		got, err := ref.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("Get %d: got %q, want %q", i, got, want)
		}
	}
}

func TestUpdate(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	key := []byte("mykey")
	if err := ref.Put(context.Background(), key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := ref.Put(context.Background(), key, []byte("v2")); err != nil {
		t.Fatal(err)
	}

	got, err := ref.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v2" {
		t.Fatalf("got %q, want v2", got)
	}
}

func TestRandomRW(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	rng := rand.New(rand.NewSource(42))
	m := make(map[string]string)
	n := 200

	for range n {
		k := fmt.Appendf(nil, "k%06d", rng.Intn(1000))
		v := fmt.Appendf(nil, "v%d", rng.Int())
		m[string(k)] = string(v)
		if err := ref.Put(context.Background(), k, v); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	for k, want := range m {
		got, err := ref.Get(context.Background(), []byte(k))
		if err != nil {
			t.Fatalf("Get %q: %v", k, err)
		}
		if string(got) != want {
			t.Fatalf("Get %q: got %q, want %q", k, got, want)
		}
	}
}

func TestSyncReopenLarge(t *testing.T) {
	dir := tempDir(t)
	n := 200
	{
		db := mustOpenDB(t, dir)
		tbl := mustTable(t, db, "test")
		ref := tbl.NewRef()
		for i := range n {
			key := fmt.Appendf(nil, "key%08d", i)
			val := fmt.Appendf(nil, "val%08d", i)
			if err := ref.Put(context.Background(), key, val); err != nil {
				t.Fatalf("Put %d: %v", i, err)
			}
		}
		ref.Sync(context.Background())
		db.Close()
	}
	{
		db := mustOpenDB(t, dir)
		defer db.Close()
		tbl := mustTable(t, db, "test")
		ref := tbl.NewRef()
		for i := range n {
			key := fmt.Appendf(nil, "key%08d", i)
			want := string(fmt.Appendf(nil, "val%08d", i))
			got, err := ref.Get(context.Background(), key)
			if err != nil {
				t.Fatalf("Get %d: %v", i, err)
			}
			if string(got) != want {
				t.Fatalf("Get %d: got %q, want %q", i, got, want)
			}
		}
	}
}

func TestIteratorAfterSync(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	n := 100
	var keys []string
	for i := range n {
		key := fmt.Appendf(nil, "k%05d", i)
		val := fmt.Appendf(nil, "v%05d", i)
		keys = append(keys, string(key))
		if err := ref.Put(context.Background(), key, val); err != nil {
			t.Fatal(err)
		}
	}
	ref.Sync(context.Background())
	sort.Strings(keys)

	it := ref.NewIterator()
	it.Seek([]byte("k00000"))
	defer it.Close()

	count := 0
	for it.Valid() {
		kv := it.Current()
		if count >= len(keys) {
			t.Fatalf("too many entries from iterator")
		}
		if string(kv.Key) != keys[count] {
			t.Fatalf("iter[%d]: got %q, want %q", count, kv.Key, keys[count])
		}
		count++
		it.Next()
	}
	if count != n {
		t.Fatalf("iterator returned %d entries, want %d", count, n)
	}
}

func TestWriteBatch(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	t1 := mustTable(t, db, "t1")
	t2 := mustTable(t, db, "t2")

	batch := db.NewBatch()
	batch.Put(t1, []byte("key1"), []byte("val1"))
	batch.Put(t2, []byte("key2"), []byte("val2"))
	batch.Delete(t1, []byte("gone"))
	if ok, err := batch.Commit(context.Background()); err != nil || !ok {
		t.Fatalf("Commit: ok=%v err=%v", ok, err)
	}

	v1, err := t1.NewRef().Get(context.Background(), []byte("key1"))
	if err != nil || string(v1) != "val1" {
		t.Fatalf("t1 key1: got %q err %v", v1, err)
	}
	v2, err := t2.NewRef().Get(context.Background(), []byte("key2"))
	if err != nil || string(v2) != "val2" {
		t.Fatalf("t2 key2: got %q err %v", v2, err)
	}
}

func TestSeqMonotonic(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)

	tbl := mustTable(t, db, "t")
	ref := tbl.NewRef()

	s0 := db.Seq()
	ref.Put(context.Background(), []byte("a"), []byte("1"))
	s1 := db.Seq()
	ref.Put(context.Background(), []byte("b"), []byte("2"))
	s2 := db.Seq()
	if !(s0 < s1 && s1 < s2) {
		t.Fatalf("seq not monotonic: %d %d %d", s0, s1, s2)
	}

	batch := db.NewBatch()
	batch.Put(tbl, []byte("c"), []byte("3"))
	batch.Put(tbl, []byte("d"), []byte("4"))
	batch.Commit(context.Background())
	s3 := db.Seq()
	if s3 <= s2 {
		t.Fatalf("batch did not advance seq: %d → %d", s2, s3)
	}

	// Reopen: seq must be restored to at least s3.
	ref.Sync(context.Background())
	db.Close()

	db2 := mustOpenDB(t, dir)
	defer db2.Close()
	if db2.Seq() < s3 {
		t.Fatalf("seq not restored after reopen: got %d, want >= %d", db2.Seq(), s3)
	}
}

func TestConditionalBatch(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	t1 := mustTable(t, db, "t1")
	t2 := mustTable(t, db, "t2")
	r1 := t1.NewRef()
	r2 := t2.NewRef()

	r1.Put(context.Background(), []byte("a"), []byte("1"))
	r2.Put(context.Background(), []byte("b"), []byte("2"))

	// Batch with correct conditions → should commit.
	batch := db.NewBatch()
	batch.Check(t1, []byte("a"), []byte("1"))
	batch.Check(t2, []byte("b"), []byte("2"))
	batch.Put(t1, []byte("a"), []byte("1x"))
	batch.Put(t2, []byte("b"), []byte("2x"))
	ok, err := batch.Commit(context.Background())
	if err != nil || !ok {
		t.Fatalf("Commit (all match): ok=%v err=%v", ok, err)
	}
	if v, _ := r1.Get(context.Background(), []byte("a")); string(v) != "1x" {
		t.Errorf("t1.a: want 1x, got %q", v)
	}
	if v, _ := r2.Get(context.Background(), []byte("b")); string(v) != "2x" {
		t.Errorf("t2.b: want 2x, got %q", v)
	}

	// Batch with one wrong condition → should not commit.
	batch2 := db.NewBatch()
	batch2.Check(t1, []byte("a"), []byte("1"))  // stale — actual is "1x"
	batch2.Check(t2, []byte("b"), []byte("2x")) // correct
	batch2.Put(t1, []byte("a"), []byte("nope"))
	batch2.Put(t2, []byte("b"), []byte("nope"))
	ok, err = batch2.Commit(context.Background())
	if err != nil || ok {
		t.Fatalf("Commit (one mismatch): ok=%v err=%v", ok, err)
	}
	if v, _ := r1.Get(context.Background(), []byte("a")); string(v) != "1x" {
		t.Errorf("t1.a after failed batch: want 1x, got %q", v)
	}
	if v, _ := r2.Get(context.Background(), []byte("b")); string(v) != "2x" {
		t.Errorf("t2.b after failed batch: want 2x, got %q", v)
	}

	// Condition requiring absence.
	batch3 := db.NewBatch()
	batch3.Check(t1, []byte("new"), nil) // must be absent
	batch3.Put(t1, []byte("new"), []byte("hello"))
	ok, err = batch3.Commit(context.Background())
	if err != nil || !ok {
		t.Fatalf("Commit (absent check): ok=%v err=%v", ok, err)
	}
	if v, _ := r1.Get(context.Background(), []byte("new")); string(v) != "hello" {
		t.Errorf("t1.new: want hello, got %q", v)
	}
}

func TestConditionalBatchAfterSync(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "t")
	ref := tbl.NewRef()
	ref.Put(context.Background(), []byte("k"), []byte("v1"))
	ref.Sync(context.Background())

	// Value is on disk; condition should still see it.
	batch := db.NewBatch()
	batch.Check(tbl, []byte("k"), []byte("v1"))
	batch.Put(tbl, []byte("k"), []byte("v2"))
	ok, err := batch.Commit(context.Background())
	if err != nil || !ok {
		t.Fatalf("Commit after sync: ok=%v err=%v", ok, err)
	}
	if v, _ := ref.Get(context.Background(), []byte("k")); string(v) != "v2" {
		t.Errorf("after conditional batch post-sync: want v2, got %q", v)
	}
}

func TestMultipleTables(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	t1 := mustTable(t, db, "t1")
	t2 := mustTable(t, db, "t2")

	t1.NewRef().Put(context.Background(), []byte("key"), []byte("val1"))
	t2.NewRef().Put(context.Background(), []byte("key"), []byte("val2"))

	v1, _ := t1.NewRef().Get(context.Background(), []byte("key"))
	v2, _ := t2.NewRef().Get(context.Background(), []byte("key"))

	if string(v1) != "val1" {
		t.Errorf("t1: want val1, got %q", v1)
	}
	if string(v2) != "val2" {
		t.Errorf("t2: want val2, got %q", v2)
	}

	tables, err := db.Tables()
	if err != nil {
		t.Fatalf("Tables: %v", err)
	}
	if len(tables) != 2 {
		t.Errorf("Tables: want 2, got %v", tables)
	}
}

func TestUpdateCAS(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	key := []byte("k")

	// Update on absent key with nil oldValue → should apply (insert).
	ok, err := ref.Update(context.Background(), key, nil, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("Update nil→v1: ok=%v err=%v", ok, err)
	}
	got, _ := ref.Get(context.Background(), key)
	if string(got) != "v1" {
		t.Fatalf("after insert: got %q, want v1", got)
	}

	// Update with wrong oldValue → should not apply.
	ok, err = ref.Update(context.Background(), key, []byte("wrong"), []byte("v2"))
	if err != nil || ok {
		t.Fatalf("Update wrong oldValue: ok=%v err=%v", ok, err)
	}
	got, _ = ref.Get(context.Background(), key)
	if string(got) != "v1" {
		t.Fatalf("after failed update: got %q, want v1", got)
	}

	// Update with correct oldValue → should apply.
	ok, err = ref.Update(context.Background(), key, []byte("v1"), []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("Update v1→v2: ok=%v err=%v", ok, err)
	}
	got, _ = ref.Get(context.Background(), key)
	if string(got) != "v2" {
		t.Fatalf("after update: got %q, want v2", got)
	}

	// Update with nil newValue → delete.
	ok, err = ref.Update(context.Background(), key, []byte("v2"), nil)
	if err != nil || !ok {
		t.Fatalf("Update v2→nil: ok=%v err=%v", ok, err)
	}
	got, _ = ref.Get(context.Background(), key)
	if got != nil {
		t.Fatalf("after delete via Update: got %q, want nil", got)
	}

	// Update on absent key with non-nil oldValue → should not apply.
	ok, err = ref.Update(context.Background(), key, []byte("v2"), []byte("v3"))
	if err != nil || ok {
		t.Fatalf("Update on absent key with non-nil oldValue: ok=%v err=%v", ok, err)
	}
}

func TestUpdateCASAfterSync(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "test")
	ref := tbl.NewRef()
	key := []byte("k")

	ref.Put(context.Background(), key, []byte("v1"))
	ref.Sync(context.Background())

	// Value is now on disk. CAS should still see it.
	ok, err := ref.Update(context.Background(), key, []byte("v1"), []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("Update after sync: ok=%v err=%v", ok, err)
	}
	got, _ := ref.Get(context.Background(), key)
	if string(got) != "v2" {
		t.Fatalf("after update post-sync: got %q, want v2", got)
	}
}

func TestDropTable(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "items")
	tbl.NewRef().Put(context.Background(), []byte("k1"), []byte("v1"))

	if err := db.DropTable(context.Background(), "items"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	tables, _ := db.Tables()
	if len(tables) != 0 {
		t.Errorf("Tables after drop: want empty, got %v", tables)
	}

	// Recreate: should be empty.
	tbl2 := mustTable(t, db, "items")
	it := tbl2.NewRef().NewIterator()
	it.Seek(nil)
	defer it.Close()
	if it.Valid() {
		t.Error("data still present after DropTable")
	}
}

// TestBlobPutGet exercises values that exceed MaxKVSize and are stored in the
// per-table blob file.
func TestBlobPutGet(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "blobs")
	ref := tbl.NewRef()

	// Build a value larger than MaxKVSize.
	large := make([]byte, MaxKVSize+1)
	for i := range large {
		large[i] = byte(i)
	}
	key := []byte("bigkey")

	if err := ref.Put(context.Background(), key, large); err != nil {
		t.Fatalf("Put large: %v", err)
	}

	// Read back from the memtable (pre-flush).
	got, err := ref.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get (memtable): %v", err)
	}
	if string(got) != string(large) {
		t.Errorf("Get (memtable): value mismatch (len %d vs %d)", len(got), len(large))
	}

	// Force flush to flexfile and read back from disk.
	db.Sync(context.Background())
	got2, err := ref.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get (disk): %v", err)
	}
	if string(got2) != string(large) {
		t.Errorf("Get (disk): value mismatch (len %d vs %d)", len(got2), len(large))
	}
}

// TestBlobReopen verifies that blob values survive a DB close/reopen cycle.
func TestBlobReopen(t *testing.T) {
	dir := tempDir(t)

	large := make([]byte, MaxKVSize*2)
	for i := range large {
		large[i] = byte(i * 3)
	}
	key := []byte("persistent-blob")

	// Write and close.
	{
		db := mustOpenDB(t, dir)
		tbl := mustTable(t, db, "t")
		if err := tbl.NewRef().Put(context.Background(), key, large); err != nil {
			t.Fatalf("Put: %v", err)
		}
		db.Close()
	}

	// Reopen and verify.
	{
		db := mustOpenDB(t, dir)
		defer db.Close()
		tbl := mustTable(t, db, "t")
		got, err := tbl.NewRef().Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get after reopen: %v", err)
		}
		if string(got) != string(large) {
			t.Errorf("Get after reopen: value mismatch (len %d vs %d)", len(got), len(large))
		}
	}
}

// TestBlobIterator verifies that iterating over a table with blob values
// transparently returns the full blob bytes.
func TestBlobIterator(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "it")
	ref := tbl.NewRef()

	blobs := map[string][]byte{
		"a": make([]byte, MaxKVSize+100),
		"b": []byte("small"),
		"c": make([]byte, MaxKVSize*3),
	}
	for i := range blobs["a"] {
		blobs["a"][i] = byte(i)
	}
	for i := range blobs["c"] {
		blobs["c"][i] = byte(i * 7)
	}

	for k, v := range blobs {
		if err := ref.Put(context.Background(), []byte(k), v); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}
	db.Sync(context.Background())

	it := ref.NewIterator()
	defer it.Close()
	it.Seek(nil)

	seen := 0
	for it.Valid() {
		kv := it.Current()
		want, ok := blobs[string(kv.Key)]
		if !ok {
			t.Errorf("unexpected key %q", kv.Key)
		} else if string(kv.Value) != string(want) {
			t.Errorf("key %q: value mismatch (len %d vs %d)", kv.Key, len(kv.Value), len(want))
		}
		seen++
		it.Next()
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if seen != len(blobs) {
		t.Errorf("saw %d keys, want %d", seen, len(blobs))
	}
}

// TestBlobDelete verifies that deleting a blob key works correctly.
func TestBlobDelete(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "del")
	ref := tbl.NewRef()

	large := make([]byte, MaxKVSize+1)
	key := []byte("toDelete")

	if err := ref.Put(context.Background(), key, large); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := ref.Delete(context.Background(), key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	db.Sync(context.Background())

	got, err := ref.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got != nil {
		t.Errorf("Get after delete: expected nil, got %d bytes", len(got))
	}
}

// TestBlobBatch verifies that Batch.Commit correctly stores and retrieves
// large values written as part of an atomic batch.
func TestBlobBatch(t *testing.T) {
	dir := tempDir(t)
	db := mustOpenDB(t, dir)
	defer db.Close()

	tbl := mustTable(t, db, "batch")
	ref := tbl.NewRef()

	large := make([]byte, MaxKVSize+256)
	for i := range large {
		large[i] = byte(i * 5)
	}

	batch := db.NewBatch()
	batch.Put(tbl, []byte("small"), []byte("tiny"))
	batch.Put(tbl, []byte("large"), large)

	ok, err := batch.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !ok {
		t.Fatal("Commit returned false")
	}

	db.Sync(context.Background())

	got, err := ref.Get(context.Background(), []byte("large"))
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	if string(got) != string(large) {
		t.Errorf("large value mismatch (len %d vs %d)", len(got), len(large))
	}
}
