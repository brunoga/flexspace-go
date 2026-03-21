# flexdb

Package `flexdb` implements a write-optimised, multi-table key-value store. It sits between the raw `flexfile` storage layer and the user-facing `flexkv` secondary-index layer.

Each **table** is an independent key-value namespace with its own `flexfile` (enabling O(1) `DropTable`). All tables share a single **double-buffered memtable** (WAL in memory + on disk), a background **flush worker**, and a configurable **per-table cache**.

---

## Architecture

```
flexkv (caller)
       │  Put / Get / Delete / NewBatch
       ▼
┌──────────────────────────────────────────────────┐
│  DB                                              │
│  ┌───────────────┐  ┌─────────────────────────┐ │
│  │ Table registry│  │ Shared memtable pair     │ │
│  │ name → Table  │  │ active ←→ immutable      │ │
│  │ id   → Table  │  │ (double-buffered WAL)    │ │
│  └───────────────┘  └──────────┬──────────────┘ │
│                                │ flush worker    │
│  ┌────────────┐  ┌────────────┐│                 │
│  │  Table 1   │  │  Table 2   ││ …               │
│  │ flexfile   │  │ flexfile   ││                 │
│  │ anchor tree│  │ anchor tree││                 │
│  │ cache 1024p│  │ cache 1024p││                 │
│  └────────────┘  └────────────┘│                 │
└──────────────────────────────────────────────────┘
```

---

## Write Path

```
TableRef.Put(key, value)
  │
  ├─ hash key → shard lock (one of 16)
  ├─ increment seqNum (atomic)
  ├─ append KV record to active memtable WAL buffer
  ├─ insert into skip list (ordered map)
  └─ release shard lock

Background flush worker (every 5 s or when memtable ≥ 1 GiB):
  ├─ acquire all 16 shard locks simultaneously
  ├─ swap active ↔ immutable
  ├─ release all 16 shard locks
  ├─ flush immutable skip list to per-table flexfiles
  │   (strip 4-byte tableID prefix, write to correct table)
  └─ truncate WAL; write fresh header with current seqNum
```

The key insight: writers never block on disk I/O. They write to the memtable (RAM + buffered WAL) and return. Disk flushes happen in the background, overlapping with subsequent writes.

## Read Path

```
TableRef.Get(key)
  ├─ probe active memtable (skip list lookup)
  ├─ probe immutable memtable (may be flushing, but still readable)
  └─ if not found in either:
      ├─ find anchor via sparse B+tree (findAnchorPos)
      ├─ check cache partition (1 of 1024 shards)
      │   ├─ cache HIT  → linear search with SIMD-style fingerprint scan
      │   └─ cache MISS → load interval from flexfile; decode KVs; insert into cache
      └─ return value or nil
```

---

## Key Data Structures

### Skip List (memtable)

The memtable uses a concurrent skip list with **p = 0.25** (25% probability of a node having a tower pointer). This means only ~6% of nodes carry any tower overhead (vs ~50% for the more common p = 0.5), keeping memory usage and GC pressure low. A fast xorshift64 PRNG is used for level selection (no interface dispatch).

The skip list stores **tableID-prefixed keys** so all tables share a single sorted structure. At flush time, the tableID prefix is stripped and each KV is written to the correct table's flexfile.

### Sparse Anchor B+tree

Each table maintains an in-memory B+tree (`dbTree`) that indexes its flexfile. Rather than indexing every key, it indexes **key intervals** (anchors). Each anchor points to a contiguous run of up to 16 sorted KV entries in the flexfile, called an **interval**.

```
dbTree leaf:  [anchor0 (key="a", loff=0, psize=120)] [anchor1 (key="m", loff=120, psize=95)] …
```

- Leaf nodes hold up to 122 anchors.
- Internal nodes hold up to 40 pivots.
- Each internal child carries a cumulative `shift` (int64) so anchor loff values stay within uint32 range.

When an interval grows beyond 16 KVs or 16 KiB in physical size, it is **split** into two anchors, each covering half the KVs.

The anchor B+tree is rebuilt from the flexfile on `Open` by scanning extent tags in parallel across 4 goroutines.

### Cache

Each table has a `dbCache` with **1 024 independent partitions**. Each partition:
- Has its own mutex (eliminates contention between most concurrent readers).
- Holds a doubly-linked circular list of `cacheEntry` objects managed by a **clock replacement** algorithm.
- Capacity is `capMB / 1024` bytes per partition.

A cache entry corresponds to one anchor interval. It holds:
- Up to 32 decoded `*KV` pointers.
- A matching array of 16-bit fingerprints (FNV-1a hash truncated to 16 bits).

**Lookup uses a SIMD-style fingerprint scan:** four 16-bit fingerprints are checked against the target in one 64-bit XOR + zero-detection pass, minimising branch mispredictions.

---

## Memtable and WAL

```
MEMTABLE_LOG0
MEMTABLE_LOG1
```

Two WAL files alternate as the active/immutable buffers. Each file begins with:
- `[timestamp:8][seqNum:8]`

Followed by variable-length records:

**Single-op record:**
```
[kv_size:8][seq:8][VI128-encoded key+value]
```

**Batch record** (atomic, all-or-nothing on crash recovery):
```
[0:8 sentinel][payload_size:8][seq:8][op_count:4]
  for each op: [kv_size:8][VI128-encoded prefixed-key+value]
```

The `seq` field is stored in the header on truncation so that `DB.Seq()` is correct even after all data is flushed and the WAL body is empty.

---

## Sequence Numbers

Every committed write (single or batch) atomically increments a global `seqNum` counter. The sequence number is:
- Stored in each WAL record.
- Restored on reopen from the WAL header.
- Accessible via `DB.Seq()`.

This provides a total order over all writes and is a foundation for MVCC and change-data-capture use cases.

---

## Atomic Batches

```go
batch := db.NewBatch()

// Optional pre-conditions (CAS semantics).
// nil expected → key must be absent.
batch.Check(table, key, expected)

// Mutations across any number of tables.
batch.Put(table1, key, value)
batch.Delete(table2, key)

seq, err := batch.Commit()
```

`Commit` acquires all 16 memtable shard locks simultaneously, evaluates all `Check` conditions, and if they all pass, applies all mutations as a single WAL record. Either the entire batch is committed or none of it is.

---

## Table Registry

The table name → ID mapping is persisted in a binary file `TABLE_REGISTRY`. It is updated atomically via a rename. Each table's data lives in:

```
db-path/
├── MEMTABLE_LOG0
├── MEMTABLE_LOG1
├── TABLE_REGISTRY
└── tables/
    ├── 1/FLEXFILE/   (DATA, LOG, FLEXTREE_META, FLEXTREE_NODE)
    ├── 2/FLEXFILE/
    └── …
```

`DropTable` calls `flexfile.Close()` and `os.RemoveAll` on the table directory — O(1) regardless of data size.

---

## Statistics

```go
stats := db.Stats()
// DBStats{Seq, TableCount, ActiveMTBytes, InactiveMTBytes}

ts := table.Stats()
// TableStats{FileBytes, AnchorCount, CacheUsed, CacheCap}
```

---

## Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `memtableCap` | 1 GiB | Triggers memtable swap |
| `memtableFlushInterval` | 5 s | Periodic flush interval |
| `memtableLogBufCap` | 4 MiB | In-memory WAL buffer per memtable |
| `memtableFlushBatch` | 1 024 | Entries per flush iteration |
| `MaxKVSize` | 4 KiB | Maximum key+value encoded size |
| `lockShards` | 16 | Memtable and flexfile lock shards |
| `cachePartitionCount` | 1 024 | Cache shards per table |
| `sparseInterval` | 32 | Max KVs per anchor interval |
| `sparseIntervalCount` | 16 | Sorted KV threshold before split |
| `sparseIntervalSize` | 16 KiB | Physical size threshold before split |
| `leafCap` | 122 | Anchors per B+tree leaf node |
| `internalCap` | 40 | Pivots per B+tree internal node |

---

## Public API

```go
// Open or create a database. capMB is the per-table cache budget.
func Open(path string, capMB uint64) (*DB, error)

// Table opens or creates a named table. Identical names return the same handle.
func (db *DB) Table(name string) (*Table, error)

// DropTable removes a table and all its data in O(1).
func (db *DB) DropTable(name string) error

// Tables returns sorted names of all open tables.
func (db *DB) Tables() ([]string, error)

// NewBatch creates an atomic multi-table write batch.
func (db *DB) NewBatch() *Batch

// Seq returns the sequence number of the last committed write.
func (db *DB) Seq() uint64

// Stats returns a snapshot of DB-level statistics.
func (db *DB) Stats() DBStats

// Sync forces a full flush of all pending writes to disk.
func (db *DB) Sync() error

// Close flushes everything and releases resources.
func (db *DB) Close() error

// NewRef creates a per-goroutine handle for a table. Must not be shared.
func (t *Table) NewRef() *TableRef

// Stats returns per-table statistics.
func (t *Table) Stats() TableStats

// TableRef methods:
func (r *TableRef) Put(key, value []byte) error
func (r *TableRef) Get(key []byte) ([]byte, error)
func (r *TableRef) Probe(key []byte) bool
func (r *TableRef) NewIterator() *Iterator
```
