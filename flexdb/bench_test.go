package flexdb

import (
	"context"
	"math/rand"
	"os"
	"testing"
)

func makeKey(i int) []byte {
	b := make([]byte, 23)
	b[0], b[1], b[2] = 'k', 'e', 'y'
	for d := 22; d >= 3; d-- {
		b[d] = byte('0' + i%10)
		i /= 10
	}
	return b
}

func makeKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := range n {
		keys[i] = makeKey(i)
	}
	return keys
}

func makeVal(vlen int) []byte {
	v := make([]byte, vlen)
	for i := range v {
		v[i] = 0xAB
	}
	return v
}

func openFresh(b *testing.B, capMB uint64) (*DB, *TableRef, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "flexdb-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	opts := DefaultOptions()
	opts.CacheMB = capMB
	db, err := Open(context.Background(), dir, opts)
	if err != nil {
		os.RemoveAll(dir)
		b.Fatal(err)
	}
	tbl, err := db.Table(context.Background(), "bench")

	if err != nil {
		db.Close()
		os.RemoveAll(dir)
		b.Fatal(err)
	}
	return db, tbl.NewRef(), dir
}

// ---- put_seq ----

func benchPutSeq(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	keys := makeKeys(b.N + 1000)

	for i := range 1000 {
		ref.Put(context.Background(), keys[i], val) //nolint:errcheck
	}

	b.ResetTimer()
	b.SetBytes(int64(23 + vlen))
	for i := 0; i < b.N; i++ {
		ref.Put(context.Background(), keys[i], val) //nolint:errcheck
	}
	b.StopTimer()
}

func BenchmarkPutSeq_v100(b *testing.B)  { benchPutSeq(b, 100) }
func BenchmarkPutSeq_v1000(b *testing.B) { benchPutSeq(b, 1000) }

// ---- put_rand ----

func benchPutRand(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	n := b.N + 1000
	keys := makeKeys(n)
	rng := rand.New(rand.NewSource(12345))
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	b.ResetTimer()
	b.SetBytes(int64(23 + vlen))
	for i := 0; i < b.N; i++ {
		ref.Put(context.Background(), keys[i], val) //nolint:errcheck
	}
	b.StopTimer()
}

func BenchmarkPutRand_v100(b *testing.B)  { benchPutRand(b, 100) }
func BenchmarkPutRand_v1000(b *testing.B) { benchPutRand(b, 1000) }

// ---- get_memtable ----

func benchGetMemtable(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	nkeys := 100000
	for i := range nkeys {
		ref.Put(context.Background(), makeKey(i), val) //nolint:errcheck
	}

	keys := makeKeys(nkeys)
	b.ResetTimer()
	b.SetBytes(int64(23 + vlen))
	for i := 0; i < b.N; i++ {
		ref.Get(context.Background(), keys[i%nkeys]) //nolint:errcheck
	}
	b.StopTimer()
}

func BenchmarkGetMemtable_v100(b *testing.B)  { benchGetMemtable(b, 100) }
func BenchmarkGetMemtable_v1000(b *testing.B) { benchGetMemtable(b, 1000) }

// ---- get_memtable_view ----

func benchGetMemtableView(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	nkeys := 100000
	for i := range nkeys {
		ref.Put(context.Background(), makeKey(i), val) //nolint:errcheck
	}

	keys := makeKeys(nkeys)
	b.ResetTimer()
	b.SetBytes(int64(23 + vlen))
	for i := 0; i < b.N; i++ {
		ref.GetView(context.Background(), keys[i%nkeys]) //nolint:errcheck
	}
	b.StopTimer()
}

func BenchmarkGetMemtableView_v100(b *testing.B)  { benchGetMemtableView(b, 100) }
func BenchmarkGetMemtableView_v1000(b *testing.B) { benchGetMemtableView(b, 1000) }

// ---- get_file ----

func benchGetFile(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	nkeys := 50000
	for i := range nkeys {
		ref.Put(context.Background(), makeKey(i), val) //nolint:errcheck
	}
	ref.Sync(context.Background()) //nolint:errcheck

	keys := makeKeys(nkeys)
	for i := range nkeys {
		ref.Get(context.Background(), keys[i]) //nolint:errcheck
	}

	b.ResetTimer()
	b.SetBytes(int64(23 + vlen))
	for i := 0; i < b.N; i++ {
		ref.Get(context.Background(), keys[i%nkeys]) //nolint:errcheck
	}
	b.StopTimer()
}

func BenchmarkGetFile_v100(b *testing.B)  { benchGetFile(b, 100) }
func BenchmarkGetFile_v1000(b *testing.B) { benchGetFile(b, 1000) }

// ---- get_file_view ----

func benchGetFileView(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	nkeys := 50000
	for i := range nkeys {
		ref.Put(context.Background(), makeKey(i), val) //nolint:errcheck
	}
	ref.Sync(context.Background()) //nolint:errcheck

	keys := makeKeys(nkeys)
	for i := range nkeys {
		ref.GetView(context.Background(), keys[i]) //nolint:errcheck
	}

	b.ResetTimer()
	b.SetBytes(int64(23 + vlen))
	for i := 0; i < b.N; i++ {
		ref.GetView(context.Background(), keys[i%nkeys]) //nolint:errcheck
	}
	b.StopTimer()
}

func BenchmarkGetFileView_v100(b *testing.B)  { benchGetFileView(b, 100) }
func BenchmarkGetFileView_v1000(b *testing.B) { benchGetFileView(b, 1000) }

// ---- delete ----

func benchDelete(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	n := b.N + 1000
	keys := makeKeys(n)
	for i := range n {
		ref.Put(context.Background(), keys[i], val) //nolint:errcheck
	}

	b.ResetTimer()
	b.SetBytes(int64(23))
	for i := 0; i < b.N; i++ {
		ref.Delete(context.Background(), keys[i]) //nolint:errcheck
	}
	b.StopTimer()
}

func BenchmarkDelete_v100(b *testing.B)  { benchDelete(b, 100) }
func BenchmarkDelete_v1000(b *testing.B) { benchDelete(b, 1000) }

// ---- iter_scan ----

func benchIterScan(b *testing.B, vlen int) {
	db, ref, dir := openFresh(b, 256)
	defer os.RemoveAll(dir)
	defer db.Close()

	val := makeVal(vlen)
	nkeys := 100000
	for i := range nkeys {
		ref.Put(context.Background(), makeKey(i), val) //nolint:errcheck
	}
	ref.Sync(context.Background()) //nolint:errcheck

	b.ResetTimer()
	b.SetBytes(int64(23 + vlen))
	count := 0
	for i := 0; i < b.N; i++ {
		it := ref.NewIterator()
		it.Seek([]byte("key"))
		for it.Valid() {
			count++
			it.Next()
		}
		it.Close()
	}
	b.StopTimer()
	_ = count
}

func BenchmarkIterScan_v100(b *testing.B)  { benchIterScan(b, 100) }
func BenchmarkIterScan_v1000(b *testing.B) { benchIterScan(b, 1000) }
