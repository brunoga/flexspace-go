package flexkv

import (
	"os"
	"testing"
)

func openTestDB(t *testing.T, path string) *DB {
	t.Helper()
	os.RemoveAll(path)
	t.Cleanup(func() { os.RemoveAll(path) })
	db, err := Open(path, &Options{CacheMB: 64})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustTable(t *testing.T, db *DB, name string) *Table {
	t.Helper()
	tbl, err := db.Table(name)
	if err != nil {
		t.Fatalf("Table(%q): %v", name, err)
	}
	return tbl
}

func mustPut(t *testing.T, tbl *Table, key, value string) {
	t.Helper()
	if err := tbl.Put([]byte(key), []byte(value)); err != nil {
		t.Fatalf("Put(%q, %q): %v", key, value, err)
	}
}

func mustDelete(t *testing.T, tbl *Table, key string) {
	t.Helper()
	if err := tbl.Delete([]byte(key)); err != nil {
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

	val, err := tbl.Get([]byte("u1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "Alice" {
		t.Errorf("Get: want Alice, got %q", val)
	}

	mustDelete(t, tbl, "u1")

	val, err = tbl.Get([]byte("u1"))
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

	if err := tbl.CreateIndex("first_letter", firstLetterIndexer); err != nil {
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
		val, err := it.GetRecord()
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

	if err := tbl.CreateIndex("name", func(_, v []byte) [][]byte {
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

	if err := tbl.CreateIndex("first_letter", firstLetterIndexer); err != nil {
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

	v1, err := t1.Get([]byte("key"))
	if err != nil {
		t.Fatalf("t1.Get: %v", err)
	}
	v2, err := t2.Get([]byte("key"))
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
	if err := tbl.CreateIndex("first_letter", firstLetterIndexer); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	mustPut(t, tbl, "k1", "Apple")
	mustPut(t, tbl, "k2", "Banana")

	if err := db.DropTable("items"); err != nil {
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
	it := tbl2.Scan(nil, nil)
	defer it.Close()
	if it.Valid() {
		t.Error("data still present after DropTable")
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
