package flexkv

import (
	"context"
	"os"
	"testing"
)

func openTestDB(t *testing.T, path string) *DB {
	t.Helper()
	os.RemoveAll(path)
	t.Cleanup(func() { os.RemoveAll(path) })
	opts := DefaultOptions()
	opts.CacheMB = 64
	db, err := Open(context.Background(), path, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
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

func mustPut(t *testing.T, tbl *Table, key, value string) {
	t.Helper()
	if err := tbl.Put(context.Background(), []byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func mustDelete(t *testing.T, tbl *Table, key string) {
	t.Helper()
	if err := tbl.Delete(context.Background(), []byte(key)); err != nil {
		t.Fatalf("Delete(%q): %v", key, err)
	}
}

// firstLetterIndexer indexes records by the first byte of their value.
var firstLetterIndexer Indexer = func(_, value []byte) [][]byte {
	if len(value) == 0 {
		return nil
	}
	return [][]byte{value[:1]}
}

func TestPutGetDelete(t *testing.T) {
	db := openTestDB(t, "test_basic")
	tbl := mustTable(t, db, "users")

	mustPut(t, tbl, "u1", "Alice")

	val, err := tbl.Get(context.Background(), []byte("u1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "Alice" {
		t.Errorf("Get: want Alice, got %q", val)
	}

	mustDelete(t, tbl, "u1")

	val, err = tbl.Get(context.Background(), []byte("u1"))
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if val != nil {
		t.Errorf("Get after Delete: want nil, got %q", val)
	}
}

func TestTableScan(t *testing.T) {
	db := openTestDB(t, "test_scan")
	tbl := mustTable(t, db, "kv")

	for _, kv := range [][2]string{
		{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"},
	} {
		mustPut(t, tbl, kv[0], kv[1])
	}

	// [b, d) should return b and c.
	it := tbl.Scan([]byte("b"), []byte("d"))
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("Scan [b,d): want [b c], got %v", got)
	}
}

func TestIndexScan(t *testing.T) {
	db := openTestDB(t, "test_index")
	tbl := mustTable(t, db, "users")

	if err := tbl.CreateIndex(context.Background(), "first_letter", firstLetterIndexer); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	for _, kv := range [][2]string{
		{"user1", "Bob"},
		{"user2", "Charlie"},
		{"user3", "David"},
		{"user4", "Bert"},
	} {
		mustPut(t, tbl, kv[0], kv[1])
	}

	// Index scan [B, C) should return Bob (user1) and Bert (user4).
	idx := tbl.Index("first_letter")
	it := idx.Scan([]byte("B"), []byte("C"))
	defer it.Close()

	wantPKs := map[string]string{
		"user1": "Bob",
		"user4": "Bert",
	}
	count := 0
	for ; it.Valid(); it.Next() {
		pk := string(it.PrimaryKey())
		val, err := it.GetRecord(context.Background())
		if err != nil {
			t.Fatalf("GetRecord: %v", err)
		}
		if wantPKs[pk] != string(val) {
			t.Errorf("index entry pk=%q: want %q, got %q", pk, wantPKs[pk], val)
		}
		count++
	}
	if count != 2 {
		t.Errorf("Scan [B,C): want 2 entries, got %d", count)
	}
}

func TestIndexScanVariableLengthValues(t *testing.T) {
	db := openTestDB(t, "test_index_varlen")
	tbl := mustTable(t, db, "items")

	if err := tbl.CreateIndex(context.Background(), "name", func(_, v []byte) [][]byte {
		return [][]byte{v}
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	mustPut(t, tbl, "k1", "Abcdefghij")
	mustPut(t, tbl, "k2", "Zz")
	mustPut(t, tbl, "k3", "Mm")

	idx := tbl.Index("name")
	it := idx.Scan([]byte("A"), []byte("Z"))
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Value()))
	}
	if len(got) != 2 {
		t.Errorf("variable-length scan [A,Z): want 2 entries, got %v", got)
	}
}

func TestIndexUpdate(t *testing.T) {
	db := openTestDB(t, "test_index_update")
	tbl := mustTable(t, db, "users")

	if err := tbl.CreateIndex(context.Background(), "first_letter", firstLetterIndexer); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	mustPut(t, tbl, "u1", "Alice") // indexed under A
	mustPut(t, tbl, "u1", "Bob")   // updated: should now be indexed under B, not A

	idx := tbl.Index("first_letter")

	// A should be empty now.
	itA := idx.Scan([]byte("A"), []byte("B"))
	defer itA.Close()
	if itA.Valid() {
		t.Error("stale index entry under A after update")
	}

	// B should have exactly one entry.
	itB := idx.Scan([]byte("B"), []byte("C"))
	defer itB.Close()
	count := 0
	for ; itB.Valid(); itB.Next() {
		count++
	}
	if count != 1 {
		t.Errorf("index under B: want 1, got %d", count)
	}
}

func TestMultipleTables(t *testing.T) {
	db := openTestDB(t, "test_multi")

	t1 := mustTable(t, db, "table1")
	t2 := mustTable(t, db, "table2")

	mustPut(t, t1, "key", "val1")
	mustPut(t, t2, "key", "val2")

	v1, err := t1.Get(context.Background(), []byte("key"))
	if err != nil {
		t.Fatalf("t1.Get: %v", err)
	}
	v2, err := t2.Get(context.Background(), []byte("key"))
	if err != nil {
		t.Fatalf("t2.Get: %v", err)
	}

	if string(v1) != "val1" {
		t.Errorf("table1: want val1, got %q", v1)
	}
	if string(v2) != "val2" {
		t.Errorf("table2: want val2, got %q", v2)
	}

	tables, err := db.Tables()
	if err != nil {
		t.Fatalf("Tables: %v", err)
	}
	if len(tables) != 2 {
		t.Errorf("Tables: want 2, got %v", tables)
	}
}

func TestDropTable(t *testing.T) {
	db := openTestDB(t, "test_drop")

	tbl := mustTable(t, db, "items")
	if err := tbl.CreateIndex(context.Background(), "first_letter", firstLetterIndexer); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	mustPut(t, tbl, "k1", "Apple")
	mustPut(t, tbl, "k2", "Banana")

	if err := db.DropTable(context.Background(), "items"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	tables, err := db.Tables()
	if err != nil {
		t.Fatalf("Tables after drop: %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("Tables after drop: want empty, got %v", tables)
	}

	// Recreate: should be empty.
	tbl2 := mustTable(t, db, "items")
	_ = tbl2
	it := tbl2.Scan(nil, nil)
	defer it.Close()
	if it.Valid() {
		t.Error("data still present after DropTable")
	}
}

func TestBatch(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t, "test_batch")
	tbl := mustTable(t, db, "data")

	// Basic: put two keys atomically, both must be visible after commit.
	b := db.NewBatch()
	b.Put(tbl, []byte("k1"), []byte("v1"))
	b.Put(tbl, []byte("k2"), []byte("v2"))
	ok, err := b.Commit(ctx)
	if !ok || err != nil {
		t.Fatalf("Commit: ok=%v err=%v", ok, err)
	}
	for _, tc := range []struct{ k, want string }{{"k1", "v1"}, {"k2", "v2"}} {
		v, err := tbl.Get(ctx, []byte(tc.k))
		if err != nil || string(v) != tc.want {
			t.Errorf("Get(%q): got %q err=%v, want %q", tc.k, v, err, tc.want)
		}
	}

	// Delete in a batch removes the key.
	b2 := db.NewBatch()
	b2.Delete(tbl, []byte("k1"))
	if ok, err := b2.Commit(ctx); !ok || err != nil {
		t.Fatalf("Commit delete: ok=%v err=%v", ok, err)
	}
	if v, _ := tbl.Get(ctx, []byte("k1")); v != nil {
		t.Errorf("k1 should be deleted, got %q", v)
	}

	// Check passes when expected value matches; writes are applied.
	b3 := db.NewBatch()
	b3.Check(tbl, []byte("k2"), []byte("v2"))
	b3.Put(tbl, []byte("k2"), []byte("v2-updated"))
	if ok, err := b3.Commit(ctx); !ok || err != nil {
		t.Fatalf("Commit with passing check: ok=%v err=%v", ok, err)
	}
	if v, _ := tbl.Get(ctx, []byte("k2")); string(v) != "v2-updated" {
		t.Errorf("k2 after checked update: got %q, want v2-updated", v)
	}

	// Check fails when expected value does not match; no writes applied.
	b4 := db.NewBatch()
	b4.Check(tbl, []byte("k2"), []byte("wrong-value"))
	b4.Put(tbl, []byte("k2"), []byte("should-not-land"))
	ok, err = b4.Commit(ctx)
	if ok || err != nil {
		t.Fatalf("Commit with failing check: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
	if v, _ := tbl.Get(ctx, []byte("k2")); string(v) != "v2-updated" {
		t.Errorf("k2 unchanged after failed check: got %q, want v2-updated", v)
	}

	// Duplicate keys in a batch: last write wins.
	b5 := db.NewBatch()
	b5.Put(tbl, []byte("dup"), []byte("first"))
	b5.Put(tbl, []byte("dup"), []byte("second"))
	if ok, err := b5.Commit(ctx); !ok || err != nil {
		t.Fatalf("Commit dedup: ok=%v err=%v", ok, err)
	}
	if v, _ := tbl.Get(ctx, []byte("dup")); string(v) != "second" {
		t.Errorf("dedup: got %q, want second", v)
	}
}

func TestScanPrefix(t *testing.T) {
	db := openTestDB(t, "test_prefix")
	tbl := mustTable(t, db, "kv")

	for _, kv := range [][2]string{
		{"user:1", "Alice"},
		{"user:2", "Bob"},
		{"item:1", "Chair"},
	} {
		mustPut(t, tbl, kv[0], kv[1])
	}

	it := tbl.ScanPrefix([]byte("user:"))
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if len(got) != 2 {
		t.Errorf("ScanPrefix user:: want 2, got %v", got)
	}
}
