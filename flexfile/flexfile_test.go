package flexfile

import (
	"bytes"
	"os"
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

	// Test Write (Update)
	newData := []byte("HELLO")
	n, err = ff.Write(newData, 0)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	n, err = ff.Read(readBuf, 0)
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
		ff.Insert(data, 0)
		ff.SetTag(0, tagValue)
		ff.Sync()
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
		ff.Read(readBuf, 0)
		if !bytes.Equal(readBuf, data) {
			t.Errorf("expected %s, got %s", string(data), string(readBuf))
		}
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
			ff.Insert(data, uint64(j*benchOpSize))
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
		ff.Insert(data, uint64(j*benchOpSize))
	}
	ff.Sync()

	buf := make([]byte, benchOpSize)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ff.Read(buf, uint64((i%benchNumOps)*benchOpSize))
	}

	b.StopTimer()
	ff.Close()
}

