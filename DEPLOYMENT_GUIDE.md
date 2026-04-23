# FlexDB Deployment & Tuning Guide

FlexDB is designed for high-throughput, low-latency workloads. Achieving optimal performance requires proper OS and filesystem configuration.

## 1. Operating System Tuning (Linux)

### Virtual Memory
FlexDB uses `mmap` for data access. Large databases may hit default OS limits.
*   **Increase Max Map Count:** `sysctl -w vm.max_map_count=262144`
*   **Dirty Ratio:** For write-heavy workloads, increase the dirty background ratio to allow the kernel to buffer more writes: `sysctl -w vm.dirty_background_ratio=10`.

### File Descriptors
Each table in FlexDB opens 4–6 files. A database with thousands of tables can easily exceed the default `ulimit`.
*   Increase the limit in `/etc/security/limits.conf`:
    ```text
    * soft nofile 65536
    * hard nofile 65536
    ```

---

## 2. Filesystem Selection

### Ext4 (Recommended Default)
Good all-around performance. Ensure `dir_index` is enabled (usually default).
*   **Mount Options:** `noatime,data=ordered`.

### XFS (Recommended for Large Scales)
XFS handles large sparse files and high concurrency better than Ext4.
*   **Mount Options:** `noatime,logbsize=256k`.

### ZFS / Btrfs
These filesystems perform their own checksumming and COW. While FlexDB is compatible, you may see a performance hit due to "double-checksumming" and fragmented COW writes.
*   If using ZFS, consider setting `recordsize=128k` to match FlexDB's max extent size.

---

## 3. FlexDB Options Tuning

Use the `flexdb.Options` struct when calling `flexdb.Open`:

| Option | Default | Description |
|--------|---------|-------------|
| `CacheMB` | 64 | Per-table cache. Increase this if you have a "hot" subset of data that fits in RAM. |
| `MemtableCap` | 1 GiB | Total size of the active write buffer. Larger values improve write throughput but increase memory usage. |
| `FlushInterval` | 5s | How often to force a flush to disk. Smaller values reduce data loss window but increase I/O pressure. |
| `LogMaxSize` | 2 GiB | When a table's WAL exceeds this, a full tree checkpoint is triggered. |

---

## 4. Hardware Considerations

*   **Storage:** NVMe SSDs are highly recommended. FlexDB's append-only block model is optimized for the way SSDs handle flash erasure cycles.
*   **Memory:** Ensure you have enough RAM to cover the `MemtableCap` (x2 for double buffering) plus approximately 1MB per open table for metadata and tracking.
