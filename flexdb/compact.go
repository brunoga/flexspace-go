package flexdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
)

const compactStateMagic = uint32(0x434F4D50) // 'COMP'

// compactEntry describes one blob value that needs its sentinel updated during
// a compaction. kvLoff is the logical file offset of the KV in the flexfile;
// newOffset is where the blob was written in the new blob file.
type compactEntry struct {
	kvLoff    uint64
	newOffset uint64
	size      uint32
	key       []byte
}

// CompactBlobs rewrites the table's blob file retaining only values referenced
// by live key-value pairs. Dead blobs left by previous deletes or overwrites are
// discarded, reclaiming disk space.
//
// The operation acquires all write locks for this table and the shared memtable,
// blocking reads and writes while it runs. It is intended to be called as an
// occasional maintenance operation, not in a hot path.
//
// Crash safety: a recovery state file (BLOBFILE.compact.state) is written before
// the flexfile is modified. If the process crashes mid-compaction, the recovery
// pass in openTable will re-apply the sentinel updates and complete the rename on
// the next database open.
func (t *Table) CompactBlobs(ctx context.Context) error {
	db := t.db
	dir := tableDir(db.path, t.id)

	// Drain the memtable so all live data is in the flexfile.
	if err := db.Sync(ctx); err != nil {
		return fmt.Errorf("compactBlobs: sync: %w", err)
	}

	// Acquire all memtable write locks to block new writes during compaction.
	for i := range db.rwMT {
		db.rwMT[i].Lock()
	}
	defer func() {
		for i := range db.rwMT {
			db.rwMT[i].Unlock()
		}
	}()

	// Acquire all flexfile write locks to block concurrent reads and flushes.
	for i := range t.rwFF {
		t.rwFF[i].Lock()
	}
	defer func() {
		for i := range t.rwFF {
			t.rwFF[i].Unlock()
		}
	}()

	// Acquire blob store lock to block concurrent blob writes.
	t.blobs.mu.Lock()
	defer t.blobs.mu.Unlock()

	// Phase 1: walk the tree and collect all live blob entries.
	type liveBlob struct {
		key     []byte
		kvLoff  uint64
		blobOff uint64
		size    uint32
	}
	var live []liveBlob

	for n := t.tree.leafHead; n != nil; n = n.next {
		for i := uint32(0); i < n.count; i++ {
			nh := nodeHandler{node: n, idx: i}
			accumulatedShift(&nh)

			a := n.anchors[i]
			anchorLoff := uint64(a.loff) + uint64(nh.shift)
			p := t.cache.partitionFor(a)
			e := p.getEntry(a, anchorLoff, t.itvbuf)

			// Sort any unsorted appends so that file order == cache order,
			// allowing correct kvLoff computation from cache entry positions.
			if a.unsorted > 0 {
				if err := t.rewriteInterval(&nh, a, e); err != nil {
					p.releaseEntry(e)
					return fmt.Errorf("compactBlobs: sort interval: %w", err)
				}
				e.clearFrag()
			}

			kvLoff := anchorLoff
			for j := 0; j < e.count; j++ {
				kv := e.kvs[j]
				if isBlobSentinel(kv.Value) {
					off, sz := decodeBlobSentinel(kv.Value)
					live = append(live, liveBlob{
						key:     dupKey(kv.Key),
						kvLoff:  kvLoff,
						blobOff: off,
						size:    sz,
					})
				}
				kvLoff += uint64(kv128EncodedSize(len(kv.Key), len(kv.Value)))
			}
			p.releaseEntry(e)
		}
	}

	if len(live) == 0 {
		return nil // no blobs at all
	}

	// Check if there's actually space to reclaim.
	var liveBytes int64
	for _, lb := range live {
		liveBytes += int64(lb.size)
	}
	if t.blobs.size <= int64(blobFileHeaderSize)+liveBytes {
		return nil // nothing to reclaim
	}

	compactPath := filepath.Join(dir, "BLOBFILE.compact")
	statePath := filepath.Join(dir, "BLOBFILE.compact.state")
	blobPath := filepath.Join(dir, "BLOBFILE")

	// Remove any orphan from a previously abandoned (pre-state) compaction.
	_ = os.Remove(compactPath)

	// Phase 2: write new blob file with only live blobs.
	compactF, err := os.OpenFile(compactPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("compactBlobs: create compact file: %w", err)
	}
	abort := func() {
		compactF.Close()
		os.Remove(compactPath)
		os.Remove(statePath)
	}

	var hdr [blobFileHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[:], blobFileMagic)
	if _, err := compactF.Write(hdr[:]); err != nil {
		abort()
		return fmt.Errorf("compactBlobs: write header: %w", err)
	}

	newOff := int64(blobFileHeaderSize)
	for i := range live {
		data := make([]byte, live[i].size)
		if _, err := t.blobs.f.ReadAt(data, int64(live[i].blobOff)); err != nil {
			abort()
			return fmt.Errorf("compactBlobs: read blob at %d: %w", live[i].blobOff, err)
		}
		if _, err := compactF.WriteAt(data, newOff); err != nil {
			abort()
			return fmt.Errorf("compactBlobs: write blob: %w", err)
		}
		live[i].blobOff = uint64(newOff) // reuse field for new offset
		newOff += int64(live[i].size)
	}

	if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, compactF.Fd(), 0, 0); errno != 0 {
		abort()
		return fmt.Errorf("compactBlobs: fdatasync compact file: %w", errno)
	}

	// Phase 3: write the crash-recovery state file BEFORE touching the flexfile.
	// If we crash after this point, recoverBlobCompaction can re-apply the sentinel
	// updates and complete the rename on the next open.
	entries := make([]compactEntry, len(live))
	for i, lb := range live {
		entries[i] = compactEntry{
			kvLoff:    lb.kvLoff,
			newOffset: lb.blobOff,
			size:      lb.size,
			key:       lb.key,
		}
	}
	if err := writeCompactState(statePath, entries); err != nil {
		abort()
		return fmt.Errorf("compactBlobs: write state: %w", err)
	}

	// Phase 4: update blob sentinels in the flexfile.
	var kvbuf [2 * MaxKVSize]byte
	for _, lb := range live {
		var sentinel [blobSentinelSize]byte
		encodeBlobSentinel(sentinel[:], lb.blobOff, lb.size)
		newKV := KV{Key: lb.key, Value: sentinel[:]}
		n := encodeKV128(kvbuf[:], &newKV)
		olen := uint64(kv128EncodedSize(len(lb.key), blobSentinelSize))
		if _, err := t.ff.Update(kvbuf[:n], lb.kvLoff, olen); err != nil {
			// The state file is on disk; recovery will re-apply on next open.
			compactF.Close()
			return fmt.Errorf("compactBlobs: update sentinel at %d: %w", lb.kvLoff, err)
		}
	}

	if err := t.ff.Sync(); err != nil {
		compactF.Close()
		return fmt.Errorf("compactBlobs: sync flexfile: %w", err)
	}

	// Phase 5: atomically replace the blob file and clean up.
	if err := os.Rename(compactPath, blobPath); err != nil {
		compactF.Close()
		return fmt.Errorf("compactBlobs: rename: %w", err)
	}
	_ = os.Remove(statePath)

	// Update in-memory blob store state.
	_ = t.blobs.f.Close()
	t.blobs.f = compactF
	t.blobs.size = newOff

	// Invalidate all cache entries so they are re-loaded with the updated sentinels.
	for n := t.tree.leafHead; n != nil; n = n.next {
		for i := uint32(0); i < n.count; i++ {
			if a := n.anchors[i]; a != nil {
				a.cacheEntry = nil
			}
		}
	}
	t.cache = newDBCache(db, t.ff, db.capMB)

	return nil
}

// recoverBlobCompaction completes or rolls back an interrupted blob compaction.
// It is called from openTable before the in-memory index is built so that the
// flexfile's sentinels are consistent with the blob file before any reads happen.
func recoverBlobCompaction(dir string, ff Storage) error {
	statePath := filepath.Join(dir, "BLOBFILE.compact.state")
	compactPath := filepath.Join(dir, "BLOBFILE.compact")
	blobPath := filepath.Join(dir, "BLOBFILE")

	stateData, err := os.ReadFile(statePath)
	if os.IsNotExist(err) {
		// No state file: either no compaction was in progress, or it crashed
		// before writing the state file (before any flexfile modification).
		// Any orphan compact file is safe to discard.
		_ = os.Remove(compactPath)
		return nil
	}
	if err != nil {
		return fmt.Errorf("recoverBlobCompaction: read state: %w", err)
	}

	entries, err := parseCompactState(stateData)
	if err != nil {
		// Corrupt state file. Discard both files; the original blob file is intact
		// because the state file is written before the flexfile is modified.
		slog.Warn("recoverBlobCompaction: corrupt state file, discarding", "dir", dir, "err", err)
		_ = os.Remove(statePath)
		_ = os.Remove(compactPath)
		return nil
	}

	// Re-apply all sentinel updates (idempotent: writes the same values again).
	var kvbuf [2 * MaxKVSize]byte
	for _, e := range entries {
		var sentinel [blobSentinelSize]byte
		encodeBlobSentinel(sentinel[:], e.newOffset, e.size)
		newKV := KV{Key: e.key, Value: sentinel[:]}
		n := encodeKV128(kvbuf[:], &newKV)
		olen := uint64(kv128EncodedSize(len(e.key), blobSentinelSize))
		if _, err := ff.Update(kvbuf[:n], e.kvLoff, olen); err != nil {
			return fmt.Errorf("recoverBlobCompaction: update sentinel at %d: %w", e.kvLoff, err)
		}
	}
	if err := ff.Sync(); err != nil {
		return fmt.Errorf("recoverBlobCompaction: sync flexfile: %w", err)
	}

	// Complete the rename if the compact file still exists.
	if _, err := os.Stat(compactPath); err == nil {
		if err := os.Rename(compactPath, blobPath); err != nil {
			return fmt.Errorf("recoverBlobCompaction: rename: %w", err)
		}
	}

	_ = os.Remove(statePath)
	return nil
}

// writeCompactState atomically writes the compaction state to path.
func writeCompactState(path string, entries []compactEntry) error {
	size := 4 + 4 // magic + count
	for _, e := range entries {
		size += 8 + 8 + 4 + 2 + len(e.key)
	}
	buf := make([]byte, size)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:], compactStateMagic)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(entries)))
	off += 4
	for _, e := range entries {
		binary.LittleEndian.PutUint64(buf[off:], e.kvLoff)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:], e.newOffset)
		off += 8
		binary.LittleEndian.PutUint32(buf[off:], e.size)
		off += 4
		binary.LittleEndian.PutUint16(buf[off:], uint16(len(e.key)))
		off += 2
		copy(buf[off:], e.key)
		off += len(e.key)
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	return os.Rename(tmp, path)
}

// parseCompactState parses the binary format written by writeCompactState.
func parseCompactState(data []byte) ([]compactEntry, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("state file too short (%d bytes)", len(data))
	}
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != compactStateMagic {
		return nil, fmt.Errorf("bad magic %08x", magic)
	}
	n := int(binary.LittleEndian.Uint32(data[4:8]))
	off := 8
	entries := make([]compactEntry, 0, n)
	for i := 0; i < n; i++ {
		if off+8+8+4+2 > len(data) {
			return nil, fmt.Errorf("truncated at entry %d", i)
		}
		kvLoff := binary.LittleEndian.Uint64(data[off:])
		off += 8
		newOffset := binary.LittleEndian.Uint64(data[off:])
		off += 8
		size := binary.LittleEndian.Uint32(data[off:])
		off += 4
		keyLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2
		if off+keyLen > len(data) {
			return nil, fmt.Errorf("truncated key at entry %d", i)
		}
		key := make([]byte, keyLen)
		copy(key, data[off:off+keyLen])
		off += keyLen
		entries = append(entries, compactEntry{
			kvLoff:    kvLoff,
			newOffset: newOffset,
			size:      size,
			key:       key,
		})
	}
	return entries, nil
}
