# FlexDB Wire Format Specification

This document describes the binary on-disk format for FlexDB components. All multi-byte integers are stored in **Big-Endian** unless otherwise specified.

## 1. TABLE_REGISTRY
Stored in the database root directory. Tracks all user-defined tables.

| Field | Size (Bytes) | Description |
|-------|--------------|-------------|
| Magic | 4 | `0x464C5852` ('FLXR') |
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
| Magic | 4 | `0x46545245` ('FTRE') (Little-Endian) |
| Version | 4 | Currently `1` (Little-Endian) |
| MaxLoff | 8 | Total logical file size (Little-Endian) |
| MaxExtentSize| 4 | Max bytes per extent (Little-Endian) |
| TreeVersion | 8 | Monotonic version counter (Little-Endian) |
| NodeCount | 8 | Total nodes in tree (Little-Endian) |
| MaxNodeID | 8 | Max ID assigned to a node (Little-Endian) |
| RootID | 8 | ID of the root node (Little-Endian) |
| Checksum | 4 | CRC32 of first 52 bytes (Little-Endian) |

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
