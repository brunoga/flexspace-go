# flexspace-go

A write-optimised, embeddable key-value store with multi-table support and secondary indexing, written in pure Go with no external dependencies.

> **Platform note:** flexspace-go requires Linux. It uses `fdatasync(2)` and `mmap(2)` directly via `syscall` for durability and zero-copy reads. Other operating systems are not currently supported.

This project is a Go port and evolution of [flexspace](https://github.com/flexible-address-space/flexspace), the original C implementation of the flexible address space storage engine. While the core data structures and algorithms remain faithful to the original, the Go implementation has diverged significantly: the architecture has been extended, the API redesigned for ergonomics, and several internal optimisations added that make it generally faster than the C version across common workloads — in particular sequential writes, scan-heavy reads, and multi-table operations.

---

## Architecture

The system is built as a clean stack of four independent packages (and 2 executables), each building on the one below:


| Package | Role |
|---------|------|
| `flextree` | Translates logical byte offsets to physical file positions via a memory-mapped B+tree. Supports insert/collapse shifting so the logical address space stays contiguous as data grows or shrinks. |
| `flexfile` | Builds a durable, virtually unbounded logical flat file on top of flextree, with chunked mmap I/O, a write-ahead log, and a background GC pass for block defragmentation. |
| `flexdb` | Adds multi-table management, a double-buffered in-memory memtable (with WAL), a background flush worker, per-table caching, atomic multi-table write batches, and monotonically increasing sequence numbers. |
| `flexkv` | The user-facing layer: named tables, `Indexer`-function-based secondary indexes maintained atomically with writes, prefix/range scan, and multi-valued index support. |
| `cmd/flexctl` | A SQLite-style CLI and REPL for administering flexkv databases without writing any code. |
| `cmd/flexsrv` | An authenticated HTTPS key-value service around flexkv. Compatible with flexctl. |
---

## Why It Is Faster Than the C Version

The Go implementation improves on the C original in several ways:

- **Double-buffered memtable with a background flush worker.** Writes land in memory and return immediately; the flush to flexfile happens concurrently. The C implementation flushed synchronously on every write above the threshold.
- **16-shard read-write locks** on both memtable and flexfile paths, reducing contention in concurrent workloads to near-zero.
- **1 024-partition clock-replacement cache.** Each partition has its own lock; cache lookups never need a global mutex.
- **Skip list with p = 0.25.** Only ~6% of nodes carry tower pointers (vs ~50% for p = 0.5), keeping the hot-path memtable small and cache-friendly.
- **Fast append path in flexfile.** When inserting at the logical end of file within the current write buffer, the extent tree is bypassed entirely — the common sequential-write case is essentially a `memcpy`.
- **Atomic multi-table batches** write all index mutations for a `Put` or `Delete` as a single WAL record, eliminating the per-index sync round-trips present in the C version.

---

## Features

- **Fully durable** — all writes go through a WAL; crash recovery replays the log on next open.
- **Multi-table** — each table has its own isolated flexfile, enabling O(1) `DropTable`.
- **Atomic multi-table writes** — `Batch.Commit()` evaluates pre-conditions and applies all mutations in one step.
- **Secondary indexes** — plug in any `func(key, value []byte) [][]byte` as an indexer; multi-valued indexes supported.
- **Prefix and range scans** — on both primary keys and secondary indexes.
- **Pluggable storage** — the `Storage` interface on each flexdb table makes it straightforward to swap flexfile for a remote or replicated backend.
- **Sequence numbers** — every committed write increments a global counter persisted in the WAL; useful for change-data-capture, MVCC extensions, and conflict detection.
- **No dependencies** — pure Go standard library only.
- **Interactive CLI** (`flexctl`) — REPL, schema management, JSON import/export, batch operations.
- **Large-value (blob) support** — values above the inline threshold are stored transparently as blobs; no API change required.

---

## Size Limits

| What | Limit | Notes |
|---|---|---|
| Inline key + value | 4 KiB | Combined size; fits in a single flexfile extent. |
| Blob value | 1 GiB | Stored out-of-line when key+value exceeds 4 KiB; transparent to callers. |
| Key | < 4 KiB | A key alone must fit in the inline threshold. |
| flexsrv `max_value_bytes` | 64 MiB default | Server-side cap on values accepted or served; overridable in `config.json`. Must not exceed 1 GiB. |

Values whose combined key+value size fits within 4 KiB are stored inline in the main table file. Larger values are automatically written to a per-table blob file and replaced by a 16-byte sentinel in the main file — the process is transparent to all `flexdb`, `flexkv`, `flexctl`, and `flexsrv` callers.

---

## Quick Start

### Using flexkv in Go

```go
import (
    "context"
    "github.com/brunoga/flexspace-go/flexkv"
)

ctx := context.Background()

// Open (or create) a database.
db, err := flexkv.Open(ctx, "/var/data/mydb", &flexkv.Options{CacheMB: 64})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Open a table.
users, err := db.Table(ctx, "users")
if err != nil {
    log.Fatal(err)
}

// Write.
if err := users.Put(ctx, []byte("alice"), []byte("engineer")); err != nil {
    log.Fatal(err)
}
if err := users.Put(ctx, []byte("bob"), []byte("designer")); err != nil {
    log.Fatal(err)
}

// Read.
val, _ := users.Get(ctx, []byte("alice"))
fmt.Println(string(val)) // engineer

// Scan all keys.
it := users.Scan(nil, nil)
defer it.Close()
for ; it.Valid(); it.Next() {
    fmt.Printf("%s -> %s\n", it.Key(), it.Value())
}

// Add a secondary index on the whole value.
err = users.CreateIndex(ctx, "by_role", func(_, v []byte) [][]byte {
    return [][]byte{v}
})

// Query the index.
idx := users.Index("by_role")
iit := idx.Get(ctx, []byte("engineer"))
defer iit.Close()
for ; iit.Valid(); iit.Next() {
    rec, _ := iit.GetRecord(ctx)
    fmt.Printf("engineer: %s (full record: %s)\n", iit.PrimaryKey(), rec)
}
```

### Atomic writes and pre-conditions

```go
rawDB := db.RawDB()
batch := rawDB.NewBatch()

// Optionally assert a pre-condition: fail the batch if alice's value isn't "engineer".
expected := "engineer"
batch.Check(usersTable.RawDataTable(), []byte("alice"), []byte(expected))

// Apply mutations across tables atomically.
batch.Put(usersTable.RawDataTable(), []byte("alice"), []byte("staff-engineer"))
batch.Put(logsTable.RawDataTable(),  []byte("2026-03-18"), []byte("alice promoted"))

committed, err := batch.Commit(ctx)
if err != nil || !committed {
    log.Println("batch rejected:", err)
}
```

---

## CLI & Remote Service

The project includes two executable tools in the `cmd/` directory:

- **`flexctl`**: A SQLite-style CLI and interactive REPL for local and remote database administration. It supports table management, CRUD operations, secondary indexing, and atomic batches.
- **`flexsrv`**: An authenticated HTTPS key-value service that exposes a `flexkv` database over the network.

For detailed usage, installation, and configuration instructions, see:
- [**`cmd/flexctl/README.md`**](./cmd/flexctl/README.md)
- [**`cmd/flexsrv/README.md`**](./cmd/flexsrv/README.md)

---

## Repository Layout

```
flexspace-go/
├── flextree/        Extent-mapped B+tree (logical↔physical address translation)
├── flexfile/        Flat logical address space with mmap I/O and WAL
├── flexdb/          Multi-table KV store (memtable, cache, batches, sequence numbers)
├── flexkv/          Secondary-index layer (named tables, Indexer functions)
├── cmd/
│   ├── flexctl/     CLI and interactive REPL
│   └── flexsrv/     Authenticated HTTPS KV service
├── go.mod
└── LICENSE
```

Each directory contains its own `README.md` with a detailed description of the package internals.

---

## Building and Testing

```bash
# Build everything
go build ./...

# Run all tests
go test ./...

# Run benchmarks
go test -bench=. -benchmem ./...

# Build the CLI
go build -o flexctl ./cmd/flexctl/
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
