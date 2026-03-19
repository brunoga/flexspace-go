# flexspace-go

A write-optimised, embeddable key-value store with multi-table support and secondary indexing, written in pure Go with no external dependencies.

This project is a Go port and evolution of [flexspace](https://github.com/flexible-address-space/flexspace), the original C implementation of the flexible address space storage engine. While the core data structures and algorithms remain faithful to the original, the Go implementation has diverged significantly: the architecture has been extended, the API redesigned for ergonomics, and several internal optimisations added that make it generally faster than the C version across common workloads — in particular sequential writes, scan-heavy reads, and multi-table operations.

---

## Architecture

The system is built as a clean stack of four independent packages, each building on the one below:

```
┌───────────────────────────────────────────────────────┐
│  cmd/flexctl  — interactive CLI and REPL               │
├───────────────────────────────────────────────────────┤
│  flexkv       — named tables + secondary indexes       │
├───────────────────────────────────────────────────────┤
│  flexdb       — multi-table store, memtable, cache     │
├───────────────────────────────────────────────────────┤
│  flexfile     — flat logical address space + WAL       │
├───────────────────────────────────────────────────────┤
│  flextree     — extent-mapped B+tree                   │
└───────────────────────────────────────────────────────┘
```

| Package | Role |
|---------|------|
| `flextree` | Translates logical byte offsets to physical file positions via a memory-mapped B+tree. Supports insert/collapse shifting so the logical address space stays contiguous as data grows or shrinks. |
| `flexfile` | Builds a durable, 800 GB logical flat file on top of flextree, with chunked mmap I/O, a write-ahead log, and a background GC pass for block defragmentation. |
| `flexdb` | Adds multi-table management, a double-buffered in-memory memtable (with WAL), a background flush worker, per-table caching, atomic multi-table write batches, and monotonically increasing sequence numbers. |
| `flexkv` | The user-facing layer: named tables, `Indexer`-function-based secondary indexes maintained atomically with writes, prefix/range scan, and multi-valued index support. |
| `cmd/flexctl` | A SQLite-style CLI and REPL for administering flexkv databases without writing any code. |

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

---

## Quick Start

### Using flexkv in Go

```go
import "github.com/brunoga/flexspace-go/flexkv"

// Open (or create) a database.
db, err := flexkv.Open("/var/data/mydb", &flexkv.Options{CacheMB: 64})
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Open a table.
users, err := db.Table("users")
if err != nil {
    log.Fatal(err)
}

// Write.
users.Put([]byte("alice"), []byte("engineer"))
users.Put([]byte("bob"),   []byte("designer"))

// Read.
val, _ := users.Get([]byte("alice"))
fmt.Println(string(val)) // engineer

// Scan all keys.
it := users.Scan(nil, nil)
defer it.Close()
for ; it.Valid(); it.Next() {
    fmt.Printf("%s -> %s\n", it.Key(), it.Value())
}

// Add a secondary index on the whole value.
err = users.CreateIndex("by_role", func(_, v []byte) [][]byte {
    return [][]byte{v}
})

// Query the index.
idx := users.Index("by_role")
iit := idx.Get([]byte("engineer"))
defer iit.Close()
for ; iit.Valid(); iit.Next() {
    fmt.Printf("engineer: %s\n", iit.PrimaryKey())
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

seq, err := batch.Commit()
if err != nil {
    log.Println("batch rejected:", err)
}
```

---

## Using flexctl

### Install

```bash
git clone https://github.com/brunoga/flexspace-go
cd flexspace-go
go build -o flexctl ./cmd/flexctl/
```

### Interactive REPL

```
$ ./flexctl /var/data/mydb
flexctl> create-table users
table "users" created
flexctl> put users alice engineer
flexctl> put users bob designer
flexctl> scan users
alice  engineer
bob    designer
flexctl> create-index users by_role exact
index "by_role" created on "users"
flexctl> index-scan users by_role engineer
alice  engineer
flexctl> stats
=== Database ===
  Path:         /var/data/mydb
  Write seq:    4
  ...
flexctl> exit
```

### One-shot commands

```bash
# Table management
./flexctl mydb create-table orders
./flexctl mydb drop-table orders

# CRUD
./flexctl mydb put  users carol "staff-engineer"
./flexctl mydb get  users carol
./flexctl mydb delete users carol

# Scanning
./flexctl mydb scan   users                  # full scan
./flexctl mydb scan   users alice carol       # range [alice, carol)
./flexctl mydb prefix users ali              # keys starting with "ali"

# Secondary indexes
./flexctl mydb create-index users by_dept field 0   # field 0, comma-delimited
./flexctl mydb create-index users by_age  field 1
./flexctl mydb create-index users by_name exact
./flexctl mydb create-index users by_pfx  prefix 3
./flexctl mydb drop-index   users by_pfx
./flexctl mydb index-scan   users by_dept engineering

# Bulk operations
./flexctl mydb dump  backup.json            # export all tables
./flexctl mydb load  backup.json            # import

# Atomic batch (JSON)
./flexctl mydb batch ops.json

# Schema management
./flexctl mydb schema dump schema.json      # export current schema
./flexctl mydb schema load schema.json      # apply schema (creates tables/indexes)

# Statistics
./flexctl mydb stats
```

### Batch file format

```json
{
  "checks": [
    { "table": "inventory", "key": "widget-A", "expected": "42" }
  ],
  "ops": [
    { "op": "put",    "table": "inventory", "key": "widget-A", "value": "41" },
    { "op": "delete", "table": "staging",   "key": "widget-A" }
  ]
}
```

If any `check` fails the entire batch is rejected without applying any `ops`.

### Schema file format

```json
{
  "version": 1,
  "tables": [
    {
      "name": "users",
      "indexes": [
        { "name": "by_dept",  "type": "field",  "field": 0 },
        { "name": "by_level", "type": "field",  "field": 1, "delim": "," },
        { "name": "by_role",  "type": "exact" },
        { "name": "by_pfx",   "type": "prefix", "length": 4 },
        { "name": "by_sfx",   "type": "suffix", "length": 4 }
      ]
    }
  ]
}
```

### Index types

| Type | Syntax | Indexed value |
|------|--------|---------------|
| `exact` | `create-index T I exact` | entire value |
| `prefix` | `create-index T I prefix N` | first N bytes |
| `suffix` | `create-index T I suffix N` | last N bytes |
| `field` | `create-index T I field N [delim]` | Nth field split by delim (default `,`) |

---

## Repository Layout

```
flexspace-go/
├── flextree/        Extent-mapped B+tree (logical↔physical address translation)
├── flexfile/        Flat logical address space with mmap I/O and WAL
├── flexdb/          Multi-table KV store (memtable, cache, batches, sequence numbers)
├── flexkv/          Secondary-index layer (named tables, Indexer functions)
├── cmd/
│   └── flexctl/     CLI and interactive REPL
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
go test -bench=. -benchmem ./flexdb/
go test -bench=. -benchmem ./flextree/

# Build the CLI
go build -o flexctl ./cmd/flexctl/
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
