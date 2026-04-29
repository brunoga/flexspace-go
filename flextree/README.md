# flextree

Package `flextree` implements an extent-mapped B+tree that translates logical byte offsets into physical file positions. It is the bottom-most layer of the flexspace-go storage stack and is used exclusively by `flexfile`.

---

## What problem does it solve?

A flat file on disk has fixed-position content: inserting bytes in the middle requires physically shifting everything after the insertion point, which is O(n). `flextree` solves this by maintaining a logical address space that is independent of the physical layout of the underlying file.

Every byte range in the logical space maps to some physical location. When you insert N bytes at logical offset L, only the **tree** is updated — physical bytes are not moved. All subsequent logical offsets simply shift by N in the mapping. This makes logical insertion and deletion O(log n) in the number of extents, regardless of data volume.

---

## Data Structures

### Extent

An extent is a contiguous run of bytes in both logical and physical space:

```
logical offset L  ──►  physical offset P  (length = len bytes)
```

Extents are encoded compactly in 16 bytes:

| Field | Bits | Description |
|-------|------|-------------|
| `loff` | 32 | Logical offset within the parent leaf (relative) |
| `len` | 32 | Byte length of this extent |
| `pTag` | 64 | Physical offset (48 bits) + metadata tag (16 bits) |

The 16-bit tag is used by `flexfile` to store block metadata (anchor flags, unsorted count).

A `holeBit` sentinel in the physical offset field marks extents that represent logical holes (deleted regions not yet GC'd in the physical file).

### Tree Node

Nodes are fixed at **1024 bytes** each (matching the C implementation for on-disk compatibility). The 992-byte data payload is a union of:

- **Leaf node:** up to 60 `extent` entries + doubly-linked list pointers (`prev`, `next`) to enable O(1) leaf traversal.
- **Internal node:** up to 30 pivot keys + 31 child pointers, each carrying a cumulative `shift` (int64). The shift allows child loff values to be stored as uint32 without overflow while still representing offsets in a file larger than 4 GiB.

Nodes are allocated in slabs of 4 096 entries for better cache locality.

### Tree

The `dbTree` struct holds:

- `root *node` — root of the B+tree
- `leafHead *node` — head of the leaf linked list (for linear traversal)
- A last-leaf cache for fast sequential appends

---

## Key Operations

### Insert

```go
tree.Insert(loff uint64, poff uint64, length uint32)
```

Inserts a new extent at logical offset `loff`. All existing extents at or beyond `loff` have their logical offsets shifted right by `length`. The shift is propagated upward through the tree by updating parent pivot values — no data is moved. If the new extent is adjacent to an existing one, they are merged.

**Fast path:** If `loff` equals the current logical end-of-file and the last leaf has room, the insert skips the tree search entirely and appends directly.

### Delete (Collapse)

```go
tree.Delete(loff uint64, length uint32)
```

Removes `length` bytes starting at `loff`. All subsequent extents shift left by `length`. Partial extents at the boundaries are split as needed.

### Query

```go
results := tree.Query(loff uint64, length uint32)
```

Returns a `[]QueryResult` covering the logical range `[loff, loff+length)`. Each result carries:
- `Poff uint64` — physical offset (or holeBit if a logical hole)
- `Len uint32` — how many bytes this result covers

### Tag operations

```go
tree.SetTag(loff uint64, tag uint16)
tree.GetTag(loff uint64) uint16
```

Stores a 16-bit metadata tag on the extent that starts at `loff`. Used by `flexfile` to mark anchor extents and track the unsorted-write count.

### Persistence

```go
tree.Sync(metaPath, nodePath string) error
tree, err = flextree.LoadTree(metaPath, nodePath)
```

The tree is persisted as two files:

- `FLEXTREE_META` — a fixed-size header with root ID, leaf-head ID, max loff, and leaf count.
- `FLEXTREE_NODE` — a flat array of 1024-byte nodes, indexed by node ID.

`Sync` uses copy-on-write: only dirty nodes are assigned new IDs and written; the metadata file is written last to guarantee crash consistency.

#### FLEXTREE_NODE binary layout (1024 bytes per node)

All fields are Little-Endian (the default for this codebase).

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0–3 | 4 | Count | Number of entries in this node |
| 4 | 1 | IsLeaf | `1` = leaf, `0` = internal |
| 5–7 | 3 | (reserved) | Zero |
| 8–15 | 8 | NodeID | Unique node identifier |
| 16–31 | 16 | (reserved) | Zero |
| 32–1023 | 992 | Payload | Leaf or internal data (see below) |

**Leaf payload** (bytes 32–1023): up to 60 extents × 16 bytes each, stored at `32 + i×16`:

| Offset within entry | Size | Field |
|---------------------|------|-------|
| 0–3 | 4 | `loff` — logical offset within the leaf (uint32) |
| 4–7 | 4 | `len` — byte length of the extent (uint32) |
| 8–15 | 8 | `pTag` — physical offset (48 bits) + metadata tag (16 bits) (uint64) |

**Internal payload** (bytes 32–1023):
- Bytes 32–271: up to 30 pivot keys × 8 bytes each (uint64), at `32 + i×8`.
- Bytes 272–751: up to 31 child entries × 16 bytes each, at `272 + i×16`:

| Offset within entry | Size | Field |
|---------------------|------|-------|
| 0–7 | 8 | `shift` — cumulative logical offset shift for child (int64) |
| 8–15 | 8 | `childID` — node ID of child (uint64) |

Nodes are stored in `FLEXTREE_NODE` at file offset `nodeID × 1024`; node 0 is unused (reserved).

---

## Performance

Benchmarks run on an Intel Core i9-9980HK @ 2.40 GHz. Go compiled with `go test -bench -benchtime=5s`; C compiled with `clang -O3 -march=native -flto`. Both use the same xorshift64 PRNG seeds and identical workloads (1 million operations per run, best of 5).

| Operation | Workload | Go (ns/op) | C (ns/op) | Go vs C |
|-----------|----------|-----------|-----------|---------|
| Insert | 50% random / 50% append, 1M extents | 237 | 241 | 1.02× faster |
| InsertAppend | 100% sequential append, 1M extents | 36 | 40 | 1.11× faster |
| Query | 1 KiB range query on 1M-extent tree | 270 | 274 | 1.01× faster |
| Delete | 64-byte collapse on 1M-extent tree | 414 | 425 | 1.03× faster |
| SetTag | Tag write on 1M-extent tree | 47 | 46 | ~equal |
| GetTag | Tag read, sequential access pattern | **13** | 46 | **3.5× faster** |

All Go benchmarks report **0 allocations/op** — the tree operates entirely on pre-allocated slab memory.

**GetTag** is 3.5× faster in Go because of the last-leaf sequential-access cache in the `Tree` struct. When `GetTag` is called repeatedly with monotonically increasing `loff` values (as in `flexfile`'s anchor scan), the cache avoids the B+tree search entirely on every hit — O(1) instead of O(log n). The C implementation performs a full tree search on every call.

---

## Correctness

`brute_force.go` contains a `BruteForce` reference implementation that stores extents in a flat sorted slice and uses linear scan for all operations. The test suite cross-checks every operation between the B+tree and the brute-force implementation to verify correctness. `crash_test.go` interrupts writes and checks that `LoadTree` always returns a valid, consistent tree.

---

## Public API

```go
// Create a new tree. maxExtentSize is the maximum logical extent length
// (must match the value used when the tree file was created).
func NewTree(maxExtentSize uint32) *Tree

// Load a previously persisted tree.
func LoadTree(metaPath, nodePath string) (*Tree, error)

// Insert an extent at logical offset loff, shifting subsequent extents.
func (t *Tree) Insert(loff, poff uint64, length uint32)

// Delete length bytes at loff, collapsing subsequent extents.
func (t *Tree) Delete(loff uint64, length uint32)

// Query returns all physical extents covering [loff, loff+length).
func (t *Tree) Query(loff uint64, length uint32) []QueryResult

// UpdatePoff remaps the physical offset of the extent at loff (used by GC).
func (t *Tree) UpdatePoff(loff, newPoff uint64, length uint32)

// SetTag / GetTag store a 16-bit metadata tag on the extent starting at loff.
func (t *Tree) SetTag(loff uint64, tag uint16)
func (t *Tree) GetTag(loff uint64) uint16

// Size returns the current maximum logical offset.
func (t *Tree) Size() uint64

// Sync persists the tree to the given meta/node file paths.
func (t *Tree) Sync(metaPath, nodePath string) error

// IterateExtents calls fn for each extent in logical order.
func (t *Tree) IterateExtents(fn func(loff, poff uint64, length uint32, tag uint16) bool)

// CheckInvariants panics if the tree violates any structural invariant (testing).
func (t *Tree) CheckInvariants()
```
