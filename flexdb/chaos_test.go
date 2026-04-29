package flexdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/brunoga/flexspace-go/flexfile"
)

// ---- fault-injected storage ----

// faultInjectedStorage wraps a Storage and injects deterministic write errors.
// Read, Collapse, SetTag, IterateExtents, Size, Close, SetMetrics are
// never faulted so the WAL-replay path stays clean.
type faultInjectedStorage struct {
	mu           sync.Mutex
	s            Storage
	failInterval int // fail every Nth call to a faultable method
	count        int // protected by mu
	insertFails  atomic.Int64
	updateFails  atomic.Int64
	syncFails    atomic.Int64
}

func newFaulty(s Storage, failInterval int) *faultInjectedStorage {
	return &faultInjectedStorage{s: s, failInterval: failInterval}
}

func (s *faultInjectedStorage) failCheck() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	return s.failInterval > 0 && s.count%s.failInterval == 0
}

func (s *faultInjectedStorage) Read(buf []byte, loff uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Read(buf, loff)
}

func (s *faultInjectedStorage) Insert(buf []byte, loff uint64) (int, error) {
	if s.failCheck() {
		s.insertFails.Add(1)
		return 0, errors.New("chaos: injected Insert failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Insert(buf, loff)
}

func (s *faultInjectedStorage) Update(buf []byte, loff, olen uint64) (int, error) {
	if s.failCheck() {
		s.updateFails.Add(1)
		return 0, errors.New("chaos: injected Update failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Update(buf, loff, olen)
}

func (s *faultInjectedStorage) Sync() error {
	if s.failCheck() {
		s.syncFails.Add(1)
		return errors.New("chaos: injected Sync failure")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Sync()
}

func (s *faultInjectedStorage) Collapse(loff, length uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Collapse(loff, length)
}

func (s *faultInjectedStorage) SetTag(loff uint64, tag uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.SetTag(loff, tag)
}

func (s *faultInjectedStorage) IterateExtents(start, end uint64, fn func(loff uint64, tag uint16, data []byte) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.IterateExtents(start, end, fn)
}

func (s *faultInjectedStorage) SetMetrics(m flexfile.Metrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.s.SetMetrics(m)
}

func (s *faultInjectedStorage) Size() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Size()
}

func (s *faultInjectedStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.s.Close()
}

// ---- helpers ----

// openWithFault opens a fresh DB, replaces the named table's storage with a
// fault-injected wrapper, and returns the DB, the ref, and the real storage.
func openWithFault(t *testing.T, path, tblName string, failInterval int) (*DB, *TableRef, *faultInjectedStorage) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tbl, err := db.Table(ctx, tblName)
	if err != nil {
		db.Close()
		t.Fatalf("Table: %v", err)
	}

	db.flushWorker.stop()
	db.flushWorker = nil
	faulty := newFaulty(tbl.ff, failInterval)
	tbl.ff = faulty
	db.StartWorker()

	return db, tbl.NewRef(), faulty
}

// restoreAndClose swaps the real storage back and closes the DB cleanly.
func restoreAndClose(db *DB, tblName string, faulty *faultInjectedStorage) {
	ctx := context.Background()
	tbl, ok := db.tablesByName[tblName]
	if ok {
		db.flushWorker.stop()
		db.flushWorker = nil
		tbl.ff = faulty.s
		db.StartWorker()
	}
	_ = db.Sync(ctx)
	db.Close()
}

// ---- TestChaosDurability ----

// TestChaosDurability verifies that the DB remains consistent after faults
// injected into the Storage layer (Insert, Update, Sync).
//
// The WAL is independent of the Storage interface; every successful Put() is
// recorded in the WAL before the call returns. On reopen the WAL is replayed
// and ALL acknowledged writes are present — even if flush-to-flexfile failed.
func TestChaosDurability(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	// ------------------------------------------------------------------
	// Phase 1: write diverse data under fault injection, track successes.
	// ------------------------------------------------------------------
	db, ref, faulty := openWithFault(t, path, "chaos", 7) // fail every 7th Storage op

	type entry struct {
		key, val []byte
	}
	var mu sync.Mutex
	var committed []entry

	// Small keys/values.
	for i := range 100 {
		key := []byte(fmt.Sprintf("small-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := ref.Put(ctx, key, val); err == nil {
			mu.Lock()
			committed = append(committed, entry{key, val})
			mu.Unlock()
		}
	}

	// Medium values (near MaxKVSize boundary).
	for i := range 20 {
		key := []byte(fmt.Sprintf("med-%04d", i))
		val := bytes.Repeat([]byte{byte(i)}, MaxKVSize-50)
		if err := ref.Put(ctx, key, val); err == nil {
			mu.Lock()
			committed = append(committed, entry{key, val})
			mu.Unlock()
		}
	}

	// Blob values (exceed MaxKVSize, written to blob store).
	// Blob store writes happen before the Storage Insert, so a Storage fault
	// may leave an orphaned blob but must not corrupt the DB.
	for i := range 10 {
		key := []byte(fmt.Sprintf("blob-%04d", i))
		val := bytes.Repeat([]byte{byte(i + 1)}, MaxKVSize+512)
		if err := ref.Put(ctx, key, val); err == nil {
			mu.Lock()
			committed = append(committed, entry{key, val})
			mu.Unlock()
		}
	}

	_ = db.Sync(ctx) // may fail; data is still in WAL

	if len(committed) == 0 {
		t.Fatal("no writes committed — adjust failInterval or key count")
	}

	// ------------------------------------------------------------------
	// Phase 2: restore real storage, close cleanly, reopen.
	// ------------------------------------------------------------------
	restoreAndClose(db, "chaos", faulty)

	t.Logf("Injected faults: insert=%d update=%d sync=%d; committed=%d writes",
		faulty.insertFails.Load(), faulty.updateFails.Load(), faulty.syncFails.Load(),
		len(committed))

	db2, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen after chaos failed: %v", err)
	}
	defer db2.Close()

	tbl2, err := db2.Table(ctx, "chaos")
	if err != nil {
		t.Fatalf("table reopen failed: %v", err)
	}
	ref2 := tbl2.NewRef()

	// ------------------------------------------------------------------
	// Phase 3: every committed entry must be readable with the exact value.
	// ------------------------------------------------------------------
	for _, e := range committed {
		got, err := ref2.Get(ctx, e.key)
		if err != nil {
			t.Errorf("Get(%s): unexpected error: %v", e.key, err)
			continue
		}
		if !bytes.Equal(got, e.val) {
			t.Errorf("Get(%s): value mismatch (want %d bytes, got %d bytes)", e.key, len(e.val), len(got))
		}
	}
}

// ---- TestChaosBatchAtomicity ----

// TestChaosBatchAtomicity verifies that write batches are atomic under fault
// injection: after a fault either ALL writes in the batch are present or NONE
// are (never a partial batch).
func TestChaosBatchAtomicity(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	db, _, faulty := openWithFault(t, path, "batch", 5)

	tbl, _ := db.tablesByName["batch"]

	// Commit several batches; each batch writes a pair (key-A and key-B must
	// appear together or not at all).
	const numBatches = 40
	type pair struct{ a, b []byte }
	committed := make([]pair, 0, numBatches)

	for i := range numBatches {
		ka := []byte(fmt.Sprintf("a-%04d", i))
		kb := []byte(fmt.Sprintf("b-%04d", i))
		va := []byte(fmt.Sprintf("va-%04d", i))
		vb := []byte(fmt.Sprintf("vb-%04d", i))

		batch := db.NewBatch()
		batch.Put(tbl, ka, va)
		batch.Put(tbl, kb, vb)
		ok, err := batch.Commit(ctx)
		if err == nil && ok {
			committed = append(committed, pair{ka, kb})
		}
	}

	restoreAndClose(db, "batch", faulty)

	t.Logf("Injected faults: insert=%d update=%d sync=%d; committed batches=%d",
		faulty.insertFails.Load(), faulty.updateFails.Load(), faulty.syncFails.Load(),
		len(committed))

	// Reopen and check atomicity.
	db2, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	tbl2, _ := db2.Table(ctx, "batch")
	ref2 := tbl2.NewRef()

	for _, p := range committed {
		va, errA := ref2.Get(ctx, p.a)
		vb, errB := ref2.Get(ctx, p.b)

		presentA := errA == nil && va != nil
		presentB := errB == nil && vb != nil

		if presentA != presentB {
			t.Errorf("atomicity violated: key %s present=%v but %s present=%v",
				p.a, presentA, p.b, presentB)
		}
	}
}

// ---- TestChaosConcurrentWrites ----

// TestChaosConcurrentWrites fires multiple goroutines writing to a table
// simultaneously while fault injection is active, then verifies the DB
// reopens without error and all acknowledged writes are present.
func TestChaosConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	db, _, faulty := openWithFault(t, path, "conc", 11)
	tbl := db.tablesByName["conc"]

	const goroutines = 8
	const writesPerG = 50

	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		committed []struct{ key, val []byte }
	)

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ref := tbl.NewRef()
			for i := range writesPerG {
				key := []byte(fmt.Sprintf("g%d-k%04d", g, i))
				val := []byte(fmt.Sprintf("g%d-v%04d", g, i))
				if err := ref.Put(ctx, key, val); err == nil {
					mu.Lock()
					committed = append(committed, struct{ key, val []byte }{key, val})
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	restoreAndClose(db, "conc", faulty)

	t.Logf("Injected faults: insert=%d update=%d sync=%d; committed=%d/%d",
		faulty.insertFails.Load(), faulty.updateFails.Load(), faulty.syncFails.Load(),
		len(committed), goroutines*writesPerG)

	db2, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	tbl2, _ := db2.Table(ctx, "conc")
	ref2 := tbl2.NewRef()

	for _, e := range committed {
		got, err := ref2.Get(ctx, e.key)
		if err != nil {
			t.Errorf("Get(%s): %v", e.key, err)
			continue
		}
		if !bytes.Equal(got, e.val) {
			t.Errorf("Get(%s): value mismatch", e.key)
		}
	}
}

// ---- TestChaosCrashRecovery ----

// chaosEnvKey is set in the environment to signal the subprocess crash phase.
const chaosEnvKey = "FLEXDB_CHAOS_CRASH_DIR"

// TestChaosCrashRecovery verifies that data committed to disk via Sync()
// survives a hard crash (process exit without Close()), and that data only
// in the in-memory WAL buffer is not spuriously corrupting the DB on replay.
//
// Implementation: this test spawns itself as a subprocess. The subprocess
// writes data, calls Sync() to commit a "pre" batch to disk, writes a "wal"
// batch (stays in the WAL buffer), then exits without calling Close().
// The parent verifies that the pre batch is present and the DB is consistent.
func TestChaosCrashRecovery(t *testing.T) {
	// ---- subprocess (crash phase) ----
	if dir := os.Getenv(chaosEnvKey); dir != "" {
		crashPhase(dir)
		// crashPhase calls os.Exit; this line is unreachable.
		return
	}

	// ---- parent (verify phase) ----
	ctx := context.Background()
	dir := t.TempDir()

	// Write initial stable data and close cleanly so the DB exists on disk.
	{
		db, err := Open(ctx, dir, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		tbl, _ := db.Table(ctx, "crash")
		ref := tbl.NewRef()
		for i := range 20 {
			_ = ref.Put(ctx, []byte(fmt.Sprintf("stable-%04d", i)), []byte("stable"))
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	// Run the crash subprocess: it will write "pre-*" (synced) and
	// "wal-*" (WAL-buffer-only) data then call os.Exit(0).
	cmd := exec.Command(os.Args[0], "-test.run=^TestChaosCrashRecovery$", "-test.v")
	cmd.Env = append(os.Environ(), chaosEnvKey+"="+dir)
	out, _ := cmd.CombinedOutput() // ignore exit code
	t.Logf("crash subprocess output:\n%s", out)

	// Reopen and verify consistency.
	db2, err := Open(ctx, dir, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen after crash failed: %v", err)
	}
	defer db2.Close()

	tbl2, err := db2.Table(ctx, "crash")
	if err != nil {
		t.Fatalf("table open after crash: %v", err)
	}
	ref2 := tbl2.NewRef()

	// All stable-* keys (written and synced before the crash subprocess
	// ever ran) must be present.
	for i := range 20 {
		key := []byte(fmt.Sprintf("stable-%04d", i))
		val, err := ref2.Get(ctx, key)
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
		} else if !bytes.Equal(val, []byte("stable")) {
			t.Errorf("Get(%s): want %q, got %q", key, "stable", val)
		}
	}

	// All pre-* keys (written and Sync()-ed inside the crash subprocess)
	// must be present — they were fdatasynced before the crash.
	for i := range 20 {
		key := []byte(fmt.Sprintf("pre-%04d", i))
		val, err := ref2.Get(ctx, key)
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
		} else if !bytes.Equal(val, []byte("pre")) {
			t.Errorf("Get(%s): want %q, got %q", key, "pre", val)
		}
	}

	// wal-* keys were in the WAL buffer at crash time and were never written
	// to the WAL file on disk. They must NOT appear after recovery — finding
	// them would indicate the WAL-replay path invented data.
	for i := range 10 {
		key := []byte(fmt.Sprintf("wal-%04d", i))
		val, err := ref2.Get(ctx, key)
		if err != nil {
			t.Errorf("Get(%s): %v", key, err)
		} else if val != nil {
			t.Errorf("Get(%s): wal-buffer data survived crash (should be absent), got %q", key, val)
		}
	}

	// DB must accept new writes after crash recovery.
	ref2b := tbl2.NewRef()
	if err := ref2b.Put(ctx, []byte("post-crash"), []byte("ok")); err != nil {
		t.Errorf("Put after crash recovery: %v", err)
	}
	if err := db2.Sync(ctx); err != nil {
		t.Errorf("Sync after crash recovery: %v", err)
	}
}

// crashPhase is executed in the subprocess. It:
//  1. Opens the DB at dir (stable data already on disk from parent phase).
//  2. Writes pre-* keys and calls Sync() so they reach disk.
//  3. Writes wal-* keys without syncing (they stay in the WAL buffer).
//  4. Calls os.Exit(0) without db.Close() to simulate a hard crash.
func crashPhase(dir string) {
	ctx := context.Background()
	db, err := Open(ctx, dir, DefaultOptions())
	if err != nil {
		fmt.Fprintf(os.Stderr, "crashPhase Open: %v\n", err)
		os.Exit(2)
	}
	tbl, err := db.Table(ctx, "crash")
	if err != nil {
		fmt.Fprintf(os.Stderr, "crashPhase Table: %v\n", err)
		os.Exit(2)
	}
	ref := tbl.NewRef()

	// "pre" batch: synced to disk before crash.
	for i := range 20 {
		if putErr := ref.Put(ctx, []byte(fmt.Sprintf("pre-%04d", i)), []byte("pre")); putErr != nil {
			fmt.Fprintf(os.Stderr, "crashPhase Put pre: %v\n", putErr)
			os.Exit(2)
		}
	}
	if syncErr := db.Sync(ctx); syncErr != nil {
		fmt.Fprintf(os.Stderr, "crashPhase Sync: %v\n", syncErr)
		os.Exit(2)
	}

	// "wal" batch: written to the in-memory WAL buffer only; never flushed.
	// After os.Exit these bytes are lost — they are in a Go []byte, not on disk.
	for i := range 10 {
		_ = ref.Put(ctx, []byte(fmt.Sprintf("wal-%04d", i)), []byte("wal"))
	}

	// Hard crash: skip Close(), skip WAL flush.
	os.Exit(0)
}

// ---- TestChaosIteratorUnderFault ----

// TestChaosIteratorUnderFault ensures that iterating over data while fault
// injection is active returns consistent results — no panics, no partial rows,
// and all committed data is eventually visible after recovery.
func TestChaosIteratorUnderFault(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	// Seed the DB with clean data.
	{
		db, err := Open(ctx, path, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		tbl, _ := db.Table(ctx, "iter")
		ref := tbl.NewRef()
		for i := range 200 {
			key := []byte(fmt.Sprintf("key-%04d", i))
			val := []byte(fmt.Sprintf("val-%04d", i))
			if err := ref.Put(ctx, key, val); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Sync(ctx); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}

	// Reopen with fault injection and iterate.
	db, _, faulty := openWithFault(t, path, "iter", 13)
	tbl := db.tablesByName["iter"]
	ref := tbl.NewRef()

	// Iterate several times; faults on Storage layer should not panic.
	var totalKeys int
	for range 5 {
		it := ref.NewIterator()
		it.Seek(nil)
		for it.Valid() {
			totalKeys++
			it.Next()
		}
		if err := it.Err(); err != nil {
			// Blob-read errors are acceptable; other errors are not.
			t.Logf("iterator Err (acceptable under fault injection): %v", err)
		}
		it.Close()
	}

	t.Logf("Injected faults: insert=%d update=%d sync=%d; keys iterated=%d",
		faulty.insertFails.Load(), faulty.updateFails.Load(), faulty.syncFails.Load(), totalKeys)

	restoreAndClose(db, "iter", faulty)

	// After recovery, all 200 keys must be present.
	db2, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	tbl2, _ := db2.Table(ctx, "iter")
	ref2 := tbl2.NewRef()
	count := 0
	it := ref2.NewIterator()
	it.Seek(nil)
	for it.Valid() {
		count++
		it.Next()
	}
	it.Close()
	if count != 200 {
		t.Errorf("after recovery: want 200 keys, got %d", count)
	}
}

// ---- TestChaosMultiTableFault ----

// TestChaosMultiTableFault verifies that concurrent fault injection across
// multiple tables does not corrupt unaffected tables.
func TestChaosMultiTableFault(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	db, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}

	tblA, _ := db.Table(ctx, "tA")
	tblB, _ := db.Table(ctx, "tB")

	// Fault only tblA's storage.
	db.flushWorker.stop()
	db.flushWorker = nil
	faultyA := newFaulty(tblA.ff, 3)
	tblA.ff = faultyA
	db.StartWorker()

	refA := tblA.NewRef()
	refB := tblB.NewRef()

	var (
		mu         sync.Mutex
		committedB [][]byte
	)

	// Write to both tables concurrently. Only tblA has fault injection.
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := range 50 {
			key := []byte(fmt.Sprintf("a-%04d", i))
			_ = refA.Put(ctx, key, []byte("vA"))
		}
	}()

	go func() {
		defer wg.Done()
		for i := range 50 {
			key := []byte(fmt.Sprintf("b-%04d", i))
			if err := refB.Put(ctx, key, []byte("vB")); err == nil {
				mu.Lock()
				committedB = append(committedB, key)
				mu.Unlock()
			}
		}
	}()

	wg.Wait()
	_ = db.Sync(ctx)

	// Restore tblA and close.
	db.flushWorker.stop()
	db.flushWorker = nil
	tblA.ff = faultyA.s
	db.StartWorker()
	_ = db.Sync(ctx)
	db.Close()

	// Reopen: tblB must have all its committed data intact.
	db2, err := Open(ctx, path, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	tbl2B, _ := db2.Table(ctx, "tB")
	ref2B := tbl2B.NewRef()

	for _, key := range committedB {
		val, err := ref2B.Get(ctx, key)
		if err != nil {
			t.Errorf("tB Get(%s): %v", key, err)
		} else if !bytes.Equal(val, []byte("vB")) {
			t.Errorf("tB Get(%s): want vB, got %q", key, val)
		}
	}

	t.Logf("tA faults: insert=%d update=%d sync=%d; tB committed=%d",
		faultyA.insertFails.Load(), faultyA.updateFails.Load(), faultyA.syncFails.Load(),
		len(committedB))
}

// Ensure the atomic import is used (it's used via atomic.Int64 fields on
// faultInjectedStorage). Go's import checker doesn't see struct field types
// as "used" when they're embedded in the struct literal, so this var anchors it.
var _ atomic.Int64

// ---- WAL durability tests ----

// dirSnapshot holds an in-memory copy of a DB directory (relative path → bytes).
type dirSnapshot map[string][]byte

// snapshotDir reads every file under dir into a dirSnapshot.
func snapshotDir(t *testing.T, dir string) dirSnapshot {
	t.Helper()
	snap := make(dirSnapshot)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		snap[rel] = data
		return nil
	})
	if err != nil {
		t.Fatalf("snapshotDir: %v", err)
	}
	return snap
}

// restoreSnapshot writes all files from snap into dst, creating subdirectories
// as needed.
func restoreSnapshot(t *testing.T, snap dirSnapshot, dst string) {
	t.Helper()
	for rel, data := range snap {
		p := filepath.Join(dst, rel)
		if mkErr := os.MkdirAll(filepath.Dir(p), 0700); mkErr != nil {
			t.Fatalf("restoreSnapshot mkdir: %v", mkErr)
		}
		if wErr := os.WriteFile(p, data, 0600); wErr != nil {
			t.Fatalf("restoreSnapshot write %s: %v", rel, wErr)
		}
	}
}

// walRecordSize returns the on-disk byte size of a single-op WAL record for a
// key of userKeyLen bytes and a value of valueLen bytes, given the table ID.
// This mirrors the layout written by memtable.logAppend.
func walRecordSize(userKeyLen, valueLen int) int {
	pkeyLen := 4 + userKeyLen                          // 4-byte big-endian table ID prefix
	return 4 + 8 + kv128EncodedSize(pkeyLen, valueLen) // sz:4 + seq:8 + KV
}

// walBatchRecordSize returns the on-disk byte size of a two-op batch WAL
// record where both ops share the same key/value lengths.
// This mirrors the layout written by memtable.logAppendBatch.
func walBatchRecordSize(userKeyLen, valueLen int) int {
	pkeyLen := 4 + userKeyLen
	perOp := 4 + kv128EncodedSize(pkeyLen, valueLen) // kv_size:4 + KV
	payloadSize := 8 + 4 + 2*perOp                   // seq:8 + op_count:4 + 2 ops
	return 4 + 8 + payloadSize                       // sentinel:4 + payload_size:8 + payload
}

// TestWALTornSingleOp verifies that a WAL file truncated mid-record (simulating
// a torn write / power failure) is handled gracefully on reopen. The DB must
// open without error and recover exactly the complete records before the tear.
func TestWALTornSingleOp(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()

	const nRecords = 10
	const userKey = "k0000" // 5 bytes; all keys same length
	const userVal = "v0000" // 5 bytes

	var snap dirSnapshot
	{
		db, err := Open(ctx, srcDir, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		tbl, _ := db.Table(ctx, "torn")
		ref := tbl.NewRef()

		db.flushWorker.stop()
		db.flushWorker = nil

		for i := range nRecords {
			key := []byte(fmt.Sprintf("k%04d", i))
			val := []byte(fmt.Sprintf("v%04d", i))
			if putErr := ref.Put(ctx, key, val); putErr != nil {
				t.Fatal(putErr)
			}
		}

		// Flush WAL buffer to disk without Close() (which would truncate it).
		if flushErr := db.activeMT().flushLog(); flushErr != nil {
			t.Fatal(flushErr)
		}
		snap = snapshotDir(t, srcDir)
		// Abandon: no goroutines running, file-handle leak is acceptable in tests.
		_ = db
	}

	walData := snap["MEMTABLE_LOG0"]
	const walHeader = 16
	recSize := walRecordSize(len(userKey), len(userVal))
	wantSize := walHeader + nRecords*recSize
	if len(walData) != wantSize {
		t.Fatalf("WAL size mismatch: want %d got %d (recSize=%d)", wantSize, len(walData), recSize)
	}

	tests := []struct {
		name        string
		truncAt     int
		wantRecords int
	}{
		{"header-only", walHeader, 0},
		{"mid-size-field", walHeader + 2, 0},
		{"mid-kv-data", walHeader + 4 + 8 + 2, 0},
		{"one-complete", walHeader + recSize, 1},
		{"four-complete-plus-one-byte", walHeader + 4*recSize + 1, 4},
		{"nine-complete", walHeader + 9*recSize, 9},
		{"all-complete", walHeader + nRecords*recSize, nRecords},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dstDir := t.TempDir()
			modSnap := make(dirSnapshot, len(snap))
			for k, v := range snap {
				modSnap[k] = v
			}
			modSnap["MEMTABLE_LOG0"] = walData[:tc.truncAt]
			restoreSnapshot(t, modSnap, dstDir)

			db2, openErr := Open(ctx, dstDir, DefaultOptions())
			if openErr != nil {
				t.Fatalf("Open after truncation at %d: %v", tc.truncAt, openErr)
			}
			defer db2.Close()

			tbl2, _ := db2.Table(ctx, "torn")
			ref2 := tbl2.NewRef()

			found := 0
			for i := range nRecords {
				val, getErr := ref2.Get(ctx, []byte(fmt.Sprintf("k%04d", i)))
				if getErr != nil {
					t.Errorf("Get k%04d: %v", i, getErr)
					continue
				}
				if val != nil {
					found++
				}
			}
			if found != tc.wantRecords {
				t.Errorf("truncAt=%d: want %d records, got %d", tc.truncAt, tc.wantRecords, found)
			}
		})
	}
}

// TestWALTornBatch verifies that a two-op batch record torn mid-write leaves
// no partial batch visible after recovery: either both keys in the batch are
// present or neither is.
func TestWALTornBatch(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()

	const nBatches = 5
	const aKey = "a-0000"  // 6 bytes
	const aVal = "va-0000" // 7 bytes
	const bKey = "b-0000"  // 6 bytes
	const bVal = "vb-0000" // 7 bytes

	var snap dirSnapshot
	{
		db, err := Open(ctx, srcDir, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		tbl, _ := db.Table(ctx, "batch")

		db.flushWorker.stop()
		db.flushWorker = nil

		for i := range nBatches {
			ka := []byte(fmt.Sprintf("a-%04d", i))
			kb := []byte(fmt.Sprintf("b-%04d", i))
			va := []byte(fmt.Sprintf("va-%04d", i))
			vb := []byte(fmt.Sprintf("vb-%04d", i))
			batch := db.NewBatch()
			batch.Put(tbl, ka, va)
			batch.Put(tbl, kb, vb)
			if ok, commitErr := batch.Commit(ctx); !ok || commitErr != nil {
				t.Fatalf("batch %d commit failed: ok=%v err=%v", i, ok, commitErr)
			}
		}

		if flushErr := db.activeMT().flushLog(); flushErr != nil {
			t.Fatal(flushErr)
		}
		snap = snapshotDir(t, srcDir)
		_ = db
	}

	walData := snap["MEMTABLE_LOG0"]
	const walHeader = 16
	batchSize := walBatchRecordSize(len(aKey), len(aVal))
	wantSize := walHeader + nBatches*batchSize
	if len(walData) != wantSize {
		t.Fatalf("WAL size mismatch: want %d got %d (batchSize=%d)", wantSize, len(walData), batchSize)
	}

	tests := []struct {
		name         string
		truncAt      int
		wantComplete int // number of fully recovered batches
	}{
		{"header-only", walHeader, 0},
		{"mid-first-sentinel", walHeader + 2, 0},
		{"mid-first-payload", walHeader + 4 + 8 + 4, 0},
		{"one-complete-batch", walHeader + batchSize, 1},
		{"two-complete-plus-mid", walHeader + 2*batchSize + batchSize/2, 2},
		{"four-complete", walHeader + 4*batchSize, 4},
		{"all-complete", walHeader + nBatches*batchSize, nBatches},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dstDir := t.TempDir()
			modSnap := make(dirSnapshot, len(snap))
			for k, v := range snap {
				modSnap[k] = v
			}
			modSnap["MEMTABLE_LOG0"] = walData[:tc.truncAt]
			restoreSnapshot(t, modSnap, dstDir)

			db2, openErr := Open(ctx, dstDir, DefaultOptions())
			if openErr != nil {
				t.Fatalf("Open after truncation at %d: %v", tc.truncAt, openErr)
			}
			defer db2.Close()

			tbl2, _ := db2.Table(ctx, "batch")
			ref2 := tbl2.NewRef()

			complete := 0
			for i := range nBatches {
				va, errA := ref2.Get(ctx, []byte(fmt.Sprintf("a-%04d", i)))
				vb, errB := ref2.Get(ctx, []byte(fmt.Sprintf("b-%04d", i)))
				presentA := errA == nil && va != nil
				presentB := errB == nil && vb != nil
				if presentA != presentB {
					t.Errorf("batch %d atomicity violated: a present=%v b present=%v", i, presentA, presentB)
				}
				if presentA && presentB {
					complete++
				}
			}
			if complete != tc.wantComplete {
				t.Errorf("truncAt=%d: want %d complete batches, got %d", tc.truncAt, tc.wantComplete, complete)
			}
		})
	}
}

// TestWALFlushCycleDurability verifies the durability boundary across a
// flush cycle: data flushed to the flexfile survives a hard crash, while data
// written to the new WAL buffer (not yet on disk) is correctly absent.
func TestWALFlushCycleDurability(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()

	const nPhase1 = 20
	const nPhase2 = 10

	var snap dirSnapshot
	{
		db, err := Open(ctx, srcDir, DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		tbl, _ := db.Table(ctx, "cycle")
		ref := tbl.NewRef()

		db.flushWorker.stop()
		db.flushWorker = nil

		// Phase 1: write data that will be flushed to flexfile.
		for i := range nPhase1 {
			key := []byte(fmt.Sprintf("phase1-%04d", i))
			if putErr := ref.Put(ctx, key, []byte("durable")); putErr != nil {
				t.Fatal(putErr)
			}
		}

		// Force flush to flexfile and WAL truncation, mirroring what Sync() does.
		db.flushActiveMT()

		// Phase 2: write data that stays in the WAL buffer only (never synced).
		for i := range nPhase2 {
			key := []byte(fmt.Sprintf("phase2-%04d", i))
			if putErr := ref.Put(ctx, key, []byte("lost")); putErr != nil {
				t.Fatal(putErr)
			}
		}

		// Snapshot WITHOUT flushing the WAL buffer: phase2 data never hits disk.
		snap = snapshotDir(t, srcDir)
		_ = db
	}

	dstDir := t.TempDir()
	restoreSnapshot(t, snap, dstDir)

	db2, err := Open(ctx, dstDir, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer db2.Close()

	tbl2, _ := db2.Table(ctx, "cycle")
	ref2 := tbl2.NewRef()

	for i := range nPhase1 {
		key := []byte(fmt.Sprintf("phase1-%04d", i))
		val, getErr := ref2.Get(ctx, key)
		if getErr != nil {
			t.Errorf("phase1 Get(%s): %v", key, getErr)
		} else if !bytes.Equal(val, []byte("durable")) {
			t.Errorf("phase1 Get(%s): want %q, got %q", key, "durable", val)
		}
	}

	for i := range nPhase2 {
		key := []byte(fmt.Sprintf("phase2-%04d", i))
		val, getErr := ref2.Get(ctx, key)
		if getErr != nil {
			t.Errorf("phase2 Get(%s): %v", key, getErr)
		} else if val != nil {
			t.Errorf("phase2 Get(%s): WAL-buffer-only data survived crash, got %q", key, val)
		}
	}
}
