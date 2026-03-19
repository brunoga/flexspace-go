package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func newTestCtx(t *testing.T) *ctx {
	t.Helper()
	dir, err := os.MkdirTemp("", "flexctl-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	c := openCtx(dir)
	t.Cleanup(func() { c.close() })
	return c
}

func run(c *ctx, args ...string) {
	runCommand(c, args[0], args[1:])
}

// collectIndexScan returns all (indexedValue, primaryKey) pairs from an index scan.
func collectIndexScan(t *testing.T, c *ctx, table, index string) [][2]string {
	t.Helper()
	tbl := c.table(table)
	idx := tbl.Index(index)
	it := idx.Scan(nil, nil)
	defer it.Close()
	var rows [][2]string
	for ; it.Valid(); it.Next() {
		rows = append(rows, [2]string{string(it.Value()), string(it.PrimaryKey())})
	}
	return rows
}

// captureStdout runs f and returns whatever it printed to stdout.
func captureStdout(f func()) string {
	r, w, _ := os.Pipe()
	old := os.Stdout
	os.Stdout = w
	f()
	w.Close()
	os.Stdout = old
	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

// TestValueGetWithIndex verifies that value get <table> <key> <index> returns
// all records whose indexed value equals key.
func TestValueGetWithIndex(t *testing.T) {
	c := newTestCtx(t)

	run(c, "table", "create", "test")
	run(c, "index", "create", "test", "by_val", "exact")
	run(c, "value", "put", "test", "alice", "engineer")
	run(c, "value", "put", "test", "bob", "engineer")
	run(c, "value", "put", "test", "carol", "manager")

	out := captureStdout(func() {
		run(c, "value", "get", "test", "engineer", "by_val")
	})

	if !strings.Contains(out, "alice") || !strings.Contains(out, "bob") {
		t.Errorf("value get with index: want alice and bob, got %q", out)
	}
	if strings.Contains(out, "carol") {
		t.Errorf("value get with index: carol should not appear, got %q", out)
	}
}

// TestValueCountWithIndex verifies that value count <table> <index> counts
// entries in the index rather than the primary table.
func TestValueCountWithIndex(t *testing.T) {
	c := newTestCtx(t)

	run(c, "table", "create", "test")
	run(c, "index", "create", "test", "by_val", "exact")
	run(c, "value", "put", "test", "alice", "engineer")
	run(c, "value", "put", "test", "bob", "engineer")
	run(c, "value", "put", "test", "carol", "manager")

	out := strings.TrimSpace(captureStdout(func() {
		run(c, "value", "count", "test", "by_val")
	}))
	if out != "3" {
		t.Errorf("value count with index: want 3, got %q", out)
	}

	// Primary count is unaffected.
	out = strings.TrimSpace(captureStdout(func() {
		run(c, "value", "count", "test")
	}))
	if out != "3" {
		t.Errorf("value count (primary): want 3, got %q", out)
	}
}

// TestIndexMaintainedAfterPut verifies that putting a value AFTER creating an
// index results in the value appearing in the index scan.
func TestIndexMaintainedAfterPut(t *testing.T) {
	c := newTestCtx(t)

	run(c, "table", "create", "test")
	run(c, "index", "create", "test", "by_val", "exact")

	// Put happens AFTER index creation — the index must be updated.
	run(c, "value", "put", "test", "bruno", "engineer")

	rows := collectIndexScan(t, c, "test", "by_val")
	if len(rows) != 1 {
		t.Fatalf("index scan after put: want 1 result, got %d", len(rows))
	}
	if rows[0][0] != "engineer" || rows[0][1] != "bruno" {
		t.Errorf("index scan: want (engineer, bruno), got %v", rows[0])
	}
}

// TestIndexCleanedUpAfterDelete verifies that deleting a value removes it from
// the index even when the index was created before the value was inserted.
func TestIndexCleanedUpAfterDelete(t *testing.T) {
	c := newTestCtx(t)

	run(c, "table", "create", "test")

	// Put before creating the index (initial scan on CreateIndex covers this).
	run(c, "value", "put", "test", "bruno", "engineer")
	run(c, "index", "create", "test", "by_val", "exact")

	// Verify the value is indexed.
	rows := collectIndexScan(t, c, "test", "by_val")
	if len(rows) != 1 {
		t.Fatalf("index scan before delete: want 1 result, got %d", len(rows))
	}

	// Delete happens AFTER index creation — the index entry must be removed.
	run(c, "value", "delete", "test", "bruno")

	rows = collectIndexScan(t, c, "test", "by_val")
	if len(rows) != 0 {
		t.Fatalf("index scan after delete: want 0 results, got %d (stale entries: %v)", len(rows), rows)
	}
}
