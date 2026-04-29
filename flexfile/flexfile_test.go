package flexfile

import (
	"bytes"
	"os"
	"syscall"
	"testing"
)

func TestFlexFileBasic(t *testing.T) {
	path := "test_flexfile"
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	ff, err := Open(path)
	if err != nil {
		t.Fatalf("failed to open flexfile: %v", err)
	}
	defer ff.Close()

	data := []byte("hello world")
	n, err := ff.Insert(data, 0)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes inserted, got %d", len(data), n)
	}

	readBuf := make([]byte, len(data))
	n, err = ff.Read(readBuf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes read, got %d", len(data), n)
	}
	if !bytes.Equal(readBuf, data) {
		t.Errorf("expected %s, got %s", string(data), string(readBuf))
	}

	// Test Tag
	err = ff.SetTag(0, 123)
	if err != nil {
		t.Fatalf("set tag failed: %v", err)
	}
	tag, err := ff.GetTag(0)
	if err != nil || tag != 123 {
		t.Errorf("expected tag 123, got %d (err: %v)", tag, err)
	}

	// Test Update (in-place overwrite)
	newData := []byte("HELLO")
	_, err = ff.Update(newData, 0, uint64(len(newData)))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	_, err = ff.Read(readBuf, 0)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(readBuf[:5]) != "HELLO" {
		t.Errorf("expected HELLO, got %s", string(readBuf[:5]))
	}

	// Test Collapse
	err = ff.Collapse(0, 6)
	if err != nil {
		t.Fatalf("collapse failed: %v", err)
	}
	if ff.tree.MaxLoff() != 5 {
		t.Errorf("expected size 5, got %d", ff.tree.MaxLoff())
	}
}

func TestFlexFilePersistence(t *testing.T) {
	path := "test_persistence"
	os.RemoveAll(path)
	defer os.RemoveAll(path)

	data := []byte("persistent data")
	tagValue := uint16(456)

	{
		ff, err := Open(path)
		if err != nil {
			t.Fatalf("failed to open flexfile: %v", err)
		}
		if _, err := ff.Insert(data, 0); err != nil {
			t.Errorf("insert failed: %v", err)
		}
		if err := ff.SetTag(0, tagValue); err != nil {
			t.Errorf("set tag failed: %v", err)
		}
		if err := ff.Sync(); err != nil {
			t.Errorf("sync failed: %v", err)
		}
		ff.Close()
	}

	{
		ff, err := Open(path)
		if err != nil {
			t.Fatalf("failed to reopen flexfile: %v", err)
		}
		defer ff.Close()

		if ff.tree.MaxLoff() != uint64(len(data)) {
			t.Errorf("expected size %d, got %d", len(data), ff.tree.MaxLoff())
		}

		tag, err := ff.GetTag(0)
		if err != nil || tag != tagValue {
			t.Errorf("expected tag %d, got %d (err: %v)", tagValue, tag, err)
		}

		readBuf := make([]byte, len(data))
		if _, err := ff.Read(readBuf, 0); err != nil {
			t.Errorf("read failed: %v", err)
		}
		if !bytes.Equal(readBuf, data) {
			t.Errorf("expected %s, got %s", string(data), string(readBuf))
		}
	}
}

// TestFlexFileWALReplay verifies that redoLog() restores data written before a
// crash (i.e. after Sync() but before a clean Close() checkpoints the tree).
func TestFlexFileWALReplay(t *testing.T) {
	path := t.TempDir()
	data1 := []byte("wal replay block one")
	data2 := []byte("wal replay block two")

	// Write data, flush both data and WAL to disk, then "crash" by closing the
	// raw file descriptors without going through Close() (which would checkpoint
	// the tree and truncate the log).
	func() {
		ff, err := Open(path)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := ff.Insert(data1, 0); err != nil {
			t.Fatalf("insert data1: %v", err)
		}
		if _, err := ff.Insert(data2, uint64(len(data1))); err != nil {
			t.Fatalf("insert data2: %v", err)
		}
		// Sync flushes data block + WAL to disk. Log threshold not crossed, so
		// no tree checkpoint is written and the LOG file is not truncated.
		if err := ff.Sync(); err != nil {
			t.Fatalf("sync: %v", err)
		}
		// Simulate crash: release resources without checkpointing or truncating.
		ff.bm.close()
		for i, chunk := range ff.chunks {
			if chunk != nil {
				_ = syscall.Munmap(chunk)
				ff.chunks[i] = nil
			}
		}
		ff.dataFile.Close()
		ff.logFile.Close()
		ff.checksumFile.Close()
	}()

	// Reopen: redoLog() must replay the WAL entries and restore the tree.
	ff, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer ff.Close()

	total := uint64(len(data1) + len(data2))
	if got := ff.Size(); got != total {
		t.Errorf("size after WAL replay: got %d, want %d", got, total)
	}
	buf := make([]byte, len(data1))
	if n, err := ff.Read(buf, 0); err != nil || n != len(data1) || !bytes.Equal(buf, data1) {
		t.Errorf("data1 after replay: got %q (n=%d, err=%v), want %q", buf, n, err, data1)
	}
	buf2 := make([]byte, len(data2))
	if n, err := ff.Read(buf2, uint64(len(data1))); err != nil || n != len(data2) || !bytes.Equal(buf2, data2) {
		t.Errorf("data2 after replay: got %q (n=%d, err=%v), want %q", buf2, n, err, data2)
	}
}

// Benchmark constants: 100 K entries × 1 KiB = 100 MiB working set.
const (
	benchNumOps = 100_000
	benchOpSize = 1024
)

func makeBenchData() []byte {
	b := make([]byte, benchOpSize)
	for i := range b {
		b[i] = 0xAA
	}
	return b
}

// BenchmarkFlexFileInsert measures sequential 1 KiB append throughput.
// A fresh file is created each round (inner-loop pattern) so every insert
// hits the hot fast-path on a clean tree.
func BenchmarkFlexFileInsert(b *testing.B) {
	data := makeBenchData()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir, _ := os.MkdirTemp("", "bench_ff_insert_*")
		ff, _ := Open(dir)
		b.StartTimer()

		for j := 0; j < benchNumOps && i < b.N; j++ {
			if _, err := ff.Insert(data, uint64(j*benchOpSize)); err != nil {
				b.Fatal(err)
			}
			i++
		}
		i--

		b.StopTimer()
		ff.Close()
		os.RemoveAll(dir)
		b.StartTimer()
	}
}

// BenchmarkFlexFileRead measures 1 KiB read throughput on a synced file,
// cycling through benchNumOps pre-populated entries.
func BenchmarkFlexFileRead(b *testing.B) {
	data := makeBenchData()
	dir, _ := os.MkdirTemp("", "bench_ff_read_*")
	defer os.RemoveAll(dir)
	ff, _ := Open(dir)
	for j := 0; j < benchNumOps; j++ {
		if _, err := ff.Insert(data, uint64(j*benchOpSize)); err != nil {
			b.Fatal(err)
		}
	}
	if err := ff.Sync(); err != nil {
		b.Fatal(err)
	}

	buf := make([]byte, benchOpSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := ff.Read(buf, uint64((i%benchNumOps)*benchOpSize)); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	ff.Close()
}
