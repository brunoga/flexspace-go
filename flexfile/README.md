# flexfile

Package `flexfile` provides a durable, flat logical address space backed by a memory-mapped data file, a write-ahead log (WAL), and a `flextree` extent map. It exposes byte-granularity `Insert`, `Collapse`, `Update`, `Read`, `SetTag`, and `GC` operations that translate logical offsets to physical file positions while maintaining crash-safety through the WAL.

`flexfile` is used internally by `flexdb` and is not intended to be used directly by application code. It satisfies the `flexdb.Storage` interface.

---

## Logical vs. Physical Layout

The logical address space is a flat stream of bytes, addressed from 0 to up to **800 GiB**. The physical data file is chunked into **4 MiB blocks**. The `flextree` extent tree maps any logical offset to the physical block and offset within it.

```
Logical space:   [0 ─────────── 800 GiB)
                  │ extent tree │
Physical blocks: [blk0][blk1][blk2]…
```

Because the mapping is maintained in the extent tree, an `Insert` at logical offset L does not require shifting physical bytes — only the tree is updated to reflect that everything after L has moved. This is what enables O(log n) logical insertion.

### Block size and mmap chunking

| Constant | Value | Description |
|----------|-------|-------------|
| `blockSize` | 4 MiB | Unit of physical allocation |
| `chunkSize` | 64 MiB | Unit of mmap window |
| `maxExtentSize` | 128 KiB | Maximum length of one logical extent |
| `logMemCap` | 64 MiB | In-memory WAL buffer before flush |
| `logMaxSize` | 2 GiB | On-disk WAL size before tree checkpoint |

The data file is accessed via `mmap` in 64 MiB chunks. Chunks are mapped on demand and unmapped on `Close`. This keeps the virtual address space footprint proportional to the actually accessed working set rather than the full file size.

---

## Write Path

```
Insert(buf, loff)
    ├─ Fast path: loff == logical EOF, within current write buffer
    │   └─ memcpy into buffer; defer mmap flush
    └─ Slow path:
        ├─ allocate physical block(s) via blockManager
        ├─ write WAL record: [op=Insert][loff][poff][length]
        ├─ update flextree: Insert(loff, poff, length)
        └─ write data bytes into physical file via mmap
```

**The fast path** (sequential appends within the current 4 MiB write buffer) bypasses the extent tree entirely. This is the dominant case for new databases and makes bulk-load throughput competitive with `memcpy`-based stores.

### WAL record format

Each WAL entry is 16 bytes:

```
[op:8][arg0:24][arg1:32][arg2/poff:64]
```

Operations: `TreeInsert`, `TreeCollapseN`, `GC`, `SetTag`. On `Open`, the WAL is replayed from the last checkpoint to restore the extent tree, then truncated and a new checkpoint is written.

---

## Read Path

```
Read(buf, loff)
    └─ tree.Query(loff, len(buf)) → []QueryResult
        ├─ for each result with valid poff: mmap read
        └─ for hole extents: zero-fill
```

---

## Block Manager

The `blockManager` tracks byte usage per physical block. On each write, bytes are placed in the **current write buffer** (a single 4 MiB block kept in memory). When the buffer fills, it is flushed to the data file, mapped into the mmap window, and a new buffer is allocated.

Block allocation is simple: allocate from the current block until full, then open a new one. The usage counters are used by GC to identify fragmented blocks.

---

## Garbage Collection

```go
ff.GC()
```

GC walks all extents in the tree. Blocks whose live data is less than 50% of their capacity are considered fragmented. Live extents from those blocks are physically relocated to new blocks, and the tree's physical mappings are updated (`UpdatePoff`). The old blocks are reclaimed for future writes.

GC moves are written to the WAL before the tree is updated, so they are safe across crashes.

---

## Durability and Crash Recovery

1. **WAL-first:** Every structural change is appended to the WAL before modifying the extent tree.
2. **Tree checkpoint:** When the WAL grows beyond 2 GiB, the extent tree is serialised to `FLEXTREE_META` + `FLEXTREE_NODE` files via copy-on-write, then the WAL is truncated.
3. **Crash recovery (`Open`):** The checkpoint files are loaded first. The WAL is then replayed from the checkpoint's sequence number. Any truncated (partial) WAL record at the end is silently discarded — partial records indicate a crash before the record was committed.

---

## Files on disk

For a flexfile at path `dir/FLEXFILE/`:

```
FLEXFILE/
├── DATA            Physical data file (sparse; up to 800 GiB apparent size)
├── LOG             Write-ahead log
├── FLEXTREE_META   Extent tree header (checkpoint)
└── FLEXTREE_NODE   Extent tree node array (checkpoint)
```

The `DATA` file is sparse: only blocks that have been written contain actual disk blocks. A freshly created table appears as a 64 MiB file to `ls` (the first mmap chunk is pre-allocated as a virtual address range) but consumes only the bytes actually written.

---

## Performance

Benchmarks run on an Intel Core i9-9980HK @ 2.40 GHz. Go compiled with `go test -bench -benchtime=5s`; C compiled with `clang -O3 -march=native -flto`. Both use identical 1 KiB payloads and 100 K operations per run (best of 5 for C).

Collapse and Update are not independently benchmarked at this layer: they are thin wrappers over `flextree.Delete` + `flextree.Insert` — already covered in `flextree`'s benchmark suite — plus a log write and a block-usage counter update.

| Operation | Workload | Go (ns/op) | C (ns/op) | Go vs C |
|-----------|----------|-----------|-----------|---------|
| Insert | 100K sequential 1 KiB appends on fresh file | 296 | 295 | ~equal |
| Read | 1 KiB read cycling through 100K synced entries | **126** | 831 | **6.6× faster** |

**Read** is 6.6× faster in Go because the Go implementation maps data in 64 MiB `mmap` windows: once a chunk is mapped, every subsequent read within it is a plain `memcpy` with no syscall overhead. The C implementation issues a `pread` syscall per read, paying the full kernel-crossing cost each time.

**Insert** throughput is identical between Go and C. Both execute the same path — memcpy into a 4 MiB in-memory block buffer, a 16-byte WAL entry into the log buffer, and an O(log n) tree update — and the compiler generates equivalent code for each.

---

## Storage Interface

`flexfile.FlexFile` satisfies the `flexdb.Storage` interface, making it possible to swap in a different backend (e.g., a replicated log or a remote block store) without changing any code above it:

```go
type Storage interface {
    Read(buf []byte, loff uint64) (int, error)
    Insert(buf []byte, loff uint64) (int, error)
    Update(buf []byte, loff, olen uint64) (int, error)
    Collapse(loff, length uint64) error
    SetTag(loff uint64, tag uint16) error
    IterateExtents(start, end uint64, fn func(loff uint64, tag uint16, data []byte) bool)
    Size() uint64
    Sync()
    Close() error
}
```
