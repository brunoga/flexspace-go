# FlexDB Wire Format Specification

This document describes the binary on-disk format for FlexDB components. All multi-byte integers are **Little-Endian** unless otherwise noted.

**Exception — prefixed sort keys:** The 4-byte `tableID` prefix prepended to every key before insertion into the skip list and memtable WAL is encoded **Big-Endian**. This is a correctness requirement: Big-Endian encoding preserves numeric order under lexicographic byte comparison, ensuring keys from table 1 always sort before table 2.

---

## 1. TABLE_REGISTRY
Stored in the database root directory. Tracks all user-defined tables.

| Field | Size (Bytes) | Description |
|-------|--------------|-------------|
| Magic | 4 | `0x46524547` ('FREG') |
| Version | 4 | Currently `1` |
| TableCount | 4 | Number of tables in registry |
| [Table Entries] | Variable | Repeated TableCount times |

### Table Entry
| Field | Size (Bytes) | Description |
|-------|--------------|-------------|
| NameLen | 2 | Length of table name |
| Name | NameLen | UTF-8 encoded table name |
| TableID | 4 | Internal numeric ID (used for file paths) |

---

## 2. FLEXTREE_META
Stored at `tables/<id>/FLEXTREE_META`. Uses a double-meta-page approach for atomic updates. Two 4096-byte pages are available; the one with the highest valid version and correct checksum is used.

| Field | Size (Bytes) | Description |
|-------|--------------|-------------|
| Magic | 4 | `0x46545245` ('FTRE') |
| Version | 4 | Currently `1` |
| MaxLoff | 8 | Total logical file size |
| MaxExtentSize | 4 | Max bytes per extent |
| TreeVersion | 8 | Monotonic version counter |
| NodeCount | 8 | Total nodes in tree |
| MaxNodeID | 8 | Max ID assigned to a node |
| RootID | 8 | ID of the root node |
| Checksum | 4 | CRC32 of first 52 bytes |

---

## 3. LOG (WAL)
Stored at `tables/<id>/LOG`. Appended during writes, truncated after tree checkpoint.

### Log Header (16 bytes)
| Field | Size (Bytes) | Description |
|-------|--------------|-------------|
| Magic | 4 | `0x464C4F47` ('FLOG') |
| Version | 4 | Currently `1` |
| Reserved | 8 | Zero padding |

### Log Entry (24 bytes)
| Field | Size (Bytes) | Description |
|-------|--------------|-------------|
| V1 | 8 | Encoded Op (2 bits) + P1 (48 bits) + P2_low (14 bits) |
| V2 | 8 | P2_high (26 bits) + P3 (30 bits) |
| CRC32 | 4 | CRC32 of V1 + V2 |
| Padding | 4 | Reserved |

---

## 4. CHECKSUMS
Stored at `tables/<id>/CHECKSUMS`. Flat array of 4-byte CRC32 values.
Indexed by `(PhysicalBlockID * (4MB / 4KB)) + PageIndex`.
A stored value of `0` means the page has not been checksummed yet.
A stored value of `0xFFFFFFFF` is a sentinel for a natural CRC32 result of 0.

---

## 5. BLOBFILE
Stored at `tables/<id>/BLOBFILE`. Append-only storage for large values (those where `len(key)+len(value) > MaxKVSize`). The file grows monotonically; no bytes are ever overwritten or reclaimed after being written.

| Offset | Size (Bytes) | Description |
|--------|--------------|-------------|
| 0–3 | 4 | Magic: `0x46424C4F` ('FBLO') |
| 4–end | Variable | Blob data, written sequentially |

Blob data is written raw (no per-blob framing). The offset stored in a `BLOB_SENTINEL` is the absolute file offset of the first byte of the blob (including the 4-byte header). The size is the exact uncompressed byte length.

Each blob is fdatasynced before the corresponding `BLOB_SENTINEL` is written to the memtable WAL, ensuring that a crash after the WAL write never leaves a dangling reference.

---

## 6. BLOB_SENTINEL
When a value exceeds the inline threshold (`len(key)+len(value) > MaxKVSize = 4 KiB`), the value is stored in `BLOBFILE` and replaced in the KV store by a 16-byte sentinel. The sentinel is stored as the value in the memtable and on-disk intervals exactly like any other value.

| Field | Size (Bytes) | Description |
|-------|--------------|-------------|
| Magic | 4 | `0xB10BB10B` |
| Offset | 8 | Absolute offset in `BLOBFILE` (includes the 8-byte file header) |
| Size | 4 | Uncompressed blob size in bytes |

A value is identified as a sentinel if and only if its length is exactly 16 bytes and its first four bytes equal `0xB10BB10B`.
