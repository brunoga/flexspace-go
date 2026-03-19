# flexkv

Package `flexkv` is the user-facing layer of the flexspace-go storage stack. It provides named tables with **secondary index support**, sitting on top of `flexdb`.

---

## Model

```
DB
└── Table "users"
    ├── data table        (primary key → value)
    ├── index "by_dept"   (field[0] → primary keys)
    └── index "by_level"  (field[1] → primary keys)
└── Table "products"
    └── data table
```

Each user table is backed by one or more `flexdb` tables:
- `"users"` — primary data (`key → value`)
- `"users.$by_dept"` — index data (`encodedValue+primaryKey → ∅`)

Index tables are hidden from `DB.Tables()`. Their naming convention (`$`) keeps them invisible to users while still being standard flexdb tables subject to the same durability guarantees.

---

## Secondary Indexes

### Indexer function

An `Indexer` is a pure function that extracts the values to index from a KV pair:

```go
type Indexer func(key, value []byte) [][]byte
```

- **Return nil** to exclude the KV from the index.
- **Return one value** for a single-valued index.
- **Return multiple values** for a multi-valued (fan-out) index.

Examples:

```go
// Index the entire value (equality lookup).
exactIndexer := func(_, value []byte) [][]byte {
    return [][]byte{value}
}

// Index only the first 4 bytes of the value.
prefixIndexer := func(_, value []byte) [][]byte {
    if len(value) < 4 { return nil }
    return [][]byte{value[:4]}
}

// Index the second comma-separated field of the value.
fieldIndexer := func(_, value []byte) [][]byte {
    parts := bytes.Split(value, []byte(","))
    if len(parts) < 2 { return nil }
    return [][]byte{bytes.TrimSpace(parts[1])}
}

// Tag-style multi-valued index: each space-separated word is an index entry.
tagsIndexer := func(_, value []byte) [][]byte {
    var tags [][]byte
    for _, tag := range bytes.Fields(value) {
        tags = append(tags, tag)
    }
    return tags
}
```

### Index key encoding

Index entries are stored with the key format:

```
nullEscape(indexedValue) + primaryKey
```

`nullEscape` encodes the indexed value so that:
1. Values of any byte content and arbitrary length sort lexicographically in the correct order.
2. The primary key appended after the terminator never interferes with comparisons.

The encoding: each `\x00` byte in the value is replaced by `\x00\xFF`; the two-byte sequence `\x00\x00` marks the end.

This means an index range scan for all keys with indexed value `v` is a single seek to `encode(v)` with an upper bound at `encode(v+1)`.

### Atomicity

Every `Put` and `Delete` that touches an indexed table builds a `flexdb` batch:

1. **Fetch the old value** (to remove stale index entries).
2. **Remove stale index entries** (one batch op per old indexed value).
3. **Insert new index entries** (one batch op per new indexed value).
4. **Write the new data record**.
5. **Commit the batch atomically.**

Either all mutations land (data + all index changes) or none do. The database is always consistent after a crash.

---

## Creating and Registering Indexes

### `CreateIndex` — new index, scans existing data

```go
err := tbl.CreateIndex("by_dept", func(_, v []byte) [][]byte {
    parts := bytes.SplitN(v, []byte(","), 2)
    return [][]byte{parts[0]}
})
```

`CreateIndex` registers the indexer in memory and then does a full scan of existing data to populate the index table. Use this when adding a new index to a table that may already contain data.

### `RegisterIndex` — restart: restore indexer without re-scanning

```go
err := tbl.RegisterIndex("by_dept", myIndexer)
```

`RegisterIndex` attaches the indexer function to an index whose data is **already on disk**. It skips the population scan. Use this on every database open after the initial `CreateIndex` call.

If you forget to call `RegisterIndex` on restart, the index data on disk will become stale because future `Put`/`Delete` calls won't maintain it.

---

## Usage

```go
import "github.com/brunoga/flexspace-go/flexkv"

// Open.
db, err := flexkv.Open("/data/myapp", &flexkv.Options{CacheMB: 64})
// ...
defer db.Close()

// Get or create a table.
users, _ := db.Table("users")

// Register existing indexes on restart.
users.RegisterIndex("by_dept", deptIndexer)
users.RegisterIndex("by_role", roleIndexer)

// CRUD.
users.Put([]byte("alice"), []byte("engineering,senior"))
users.Put([]byte("bob"),   []byte("design,mid"))
val, _ := users.Get([]byte("alice"))
users.Delete([]byte("bob"))

// Primary key scan: all users.
it := users.Scan(nil, nil)
defer it.Close()
for ; it.Valid(); it.Next() {
    fmt.Printf("%s = %s\n", it.Key(), it.Value())
}

// Prefix scan.
it2 := users.ScanPrefix([]byte("ali"))
defer it2.Close()

// Range scan: keys in [alice, carol).
it3 := users.Scan([]byte("alice"), []byte("carol"))
defer it3.Close()

// Index lookup: all engineers.
idx := users.Index("by_dept")
iit := idx.Get([]byte("engineering"))
defer iit.Close()
for ; iit.Valid(); iit.Next() {
    pk := iit.PrimaryKey()
    val, _ := iit.GetRecord() // fetches full value by pk
    fmt.Printf("%s: %s\n", pk, val)
}

// Index range scan: dept between "design" (inclusive) and "engineering" (exclusive).
iit2 := idx.Scan([]byte("design"), []byte("engineering"))
defer iit2.Close()

// Drop an index.
users.DropIndex("by_dept")

// Drop a table (and all its indexes).
db.DropTable("users")

// List user tables.
names, _ := db.Tables() // ["users", "products", ...]
```

---

## Diagnostics

```go
// Access the underlying flexdb.DB for low-level stats or batch operations.
fdb := db.RawDB()
stats := fdb.Stats() // flexdb.DBStats

// Access the flexdb table backing a user table's primary data.
raw := tbl.RawDataTable()
ts := raw.Stats() // flexdb.TableStats

// Access the flexdb table for a named index.
ft, ok := tbl.RawIndexTable("by_dept")
if ok {
    its := ft.Stats()
}
```

---

## Thread Safety

`DB` and `Table` methods are safe for concurrent use. The indexer map within each table is protected by a `sync.RWMutex`. The underlying `flexdb` tables use 16-shard locks on both the memtable and flexfile paths. Multiple goroutines can call `Put`, `Get`, `Scan`, and index lookups concurrently without additional synchronisation.

---

## Schema persistence

`flexkv` does not persist indexer **functions** — they are code, not data. The index **data** is persisted on disk in the hidden `$`-prefixed flexdb tables. Index definitions (name, type, parameters) must be saved externally if you need them to survive a process restart. The `flexctl` tool stores them in a `_schema` table within the same database.

---

## Public API

```go
// DB
func Open(path string, opts *Options) (*DB, error)
func (db *DB) Table(name string) (*Table, error)
func (db *DB) DropTable(name string) error
func (db *DB) Tables() ([]string, error)
func (db *DB) Close() error
func (db *DB) RawDB() *flexdb.DB

// Table
func (t *Table) Put(key, value []byte) error
func (t *Table) Get(key []byte) ([]byte, error)
func (t *Table) Delete(key []byte) error
func (t *Table) CreateIndex(name string, indexer Indexer) error
func (t *Table) RegisterIndex(name string, indexer Indexer) error
func (t *Table) DropIndex(name string) error
func (t *Table) Scan(start, end []byte) *Iterator
func (t *Table) ScanPrefix(prefix []byte) *Iterator
func (t *Table) Index(name string) *Index
func (t *Table) RawDataTable() *flexdb.Table
func (t *Table) RawIndexTable(name string) (*flexdb.Table, bool)

// Iterator (primary key space)
func (it *Iterator) Valid() bool
func (it *Iterator) Next()
func (it *Iterator) Key() []byte
func (it *Iterator) Value() []byte
func (it *Iterator) Close()

// Index
func (idx *Index) Scan(start, end []byte) *IndexIterator
func (idx *Index) Get(value []byte) *IndexIterator

// IndexIterator
func (it *IndexIterator) Valid() bool
func (it *IndexIterator) Next()
func (it *IndexIterator) Value() []byte       // indexed value
func (it *IndexIterator) PrimaryKey() []byte
func (it *IndexIterator) GetRecord() ([]byte, error)
func (it *IndexIterator) Close()
```
