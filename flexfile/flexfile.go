// Package flexfile implements a flat logical address space backed by a memory-
// mapped data file, a write-ahead log (WAL), and a flextree extent map.
// It provides Insert, Collapse, Update, Read, SetTag, and GC operations that
// translate logical offsets to physical file positions, maintaining durability
// through the WAL. Safe for concurrent use.
package flexfile

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/brunoga/flexspace-go/flextree"
)

var (
	writeCount uint64
	syncCount  uint64
)

const (
	// blockBits defines the block size exponent (block size = 4 MiB).
	blockBits = 22
	// blockSize is the physical block size in bytes (4 MiB).
	blockSize = 1 << blockBits

	// maxExtentBit is used to calculate maxExtentSize.
	maxExtentBit = 5
	// maxExtentSize is the maximum logical extent size in bytes (128 KiB).
	maxExtentSize = blockSize >> maxExtentBit

	// logMemCap is the in-memory log buffer capacity (64 MiB).
	logMemCap = 64 << 20
	// logMaxSize is the maximum on-disk log size before a tree checkpoint (2 GiB).
	logMaxSize = 2 << 30

	// gcThreshold is the minimum number of free blocks below which GC is triggered.
	gcThreshold = 64

	// chunkBits defines the mmap chunk size exponent (64 MiB per chunk).
	// 64 MiB = 16× the 4 MiB block size: large enough to avoid frequent
	// remapping, small enough to limit on-disk padding for small databases.
	chunkBits = 26
	// chunkSize is the mmap chunk size in bytes (64 MiB).
	chunkSize = 1 << chunkBits
)

type op uint8

const (
	opTreeInsert    op = 0
	opTreeCollapseN op = 1
	opGC            op = 2
	opSetTag        op = 3
)

const (
	logMagic     = 0x464C4F47 // 'FLOG'
	logVersion   = 1
	logEntrySize = 24

	pageSize     = 4096
	checksumSize = 4 // CRC32 per page
)

type FlexFile struct {
	path         string
	tree         *flextree.Tree
	dataFile     *os.File
	logFile      *os.File
	checksumFile *os.File

	mu sync.Mutex

	logBuf       []byte
	logBufBase   unsafe.Pointer
	logBufSize   uint32
	logTotalSize uint64

	chunks       [][]byte
	lastChunkIdx uint64
	lastChunk    []byte

	bm      *blockManager
	metrics Metrics
}

// SetMetrics attaches a metrics collector to the flexfile.
func (ff *FlexFile) SetMetrics(m Metrics) {
	ff.metrics = m
}

// Open opens or creates a flexfile at the given path.
func Open(path string) (*FlexFile, error) {
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	dataFilePath := filepath.Join(path, "DATA")
	dataFile, err := os.OpenFile(dataFilePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	treeMetaPath := filepath.Join(path, "FLEXTREE_META")
	treeNodePath := filepath.Join(path, "FLEXTREE_NODE")

	var tree *flextree.Tree
	if _, err := os.Stat(treeMetaPath); err == nil {
		tree, err = flextree.LoadTree(treeMetaPath, treeNodePath)
		if err != nil {
			dataFile.Close()
			return nil, fmt.Errorf("failed to load tree: %w", err)
		}
	} else {
		tree = flextree.NewTree(maxExtentSize)
	}

	logFilePath := filepath.Join(path, "LOG")
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	checksumFilePath := filepath.Join(path, "CHECKSUMS")
	checksumFile, err := os.OpenFile(checksumFilePath, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		dataFile.Close()
		logFile.Close()
		return nil, fmt.Errorf("failed to open checksum file: %w", err)
	}

	logBuf := make([]byte, logMemCap)
	ff := &FlexFile{
		path:         path,
		tree:         tree,
		dataFile:     dataFile,
		logFile:      logFile,
		checksumFile: checksumFile,
		logBuf:       logBuf,
		logBufBase:   unsafe.Pointer(&logBuf[0]),
		chunks:       make([][]byte, 16),
		lastChunkIdx: ^uint64(0),
	}
	if err := ff.redoLog(); err != nil {
		ff.Close()
		return nil, fmt.Errorf("failed to redo log: %w", err)
	}

	ff.bm = newBlockManager(ff)
	if err := ff.bm.init(tree); err != nil {
		ff.Close()
		return nil, fmt.Errorf("failed to initialize block manager: %w", err)
	}

	return ff, nil
}

func (ff *FlexFile) redoLog() error {
	info, err := ff.logFile.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		return nil
	}

	totalSize := info.Size()
	fullBuf := make([]byte, totalSize)
	if _, err := ff.logFile.ReadAt(fullBuf, 0); err != nil {
		return err
	}

	off := int64(0)
	entrySize := int64(logEntrySize)
	versioned := false
	if totalSize >= 16 {
		magic := binary.BigEndian.Uint32(fullBuf[0:4])
		if magic == logMagic {
			version := binary.BigEndian.Uint32(fullBuf[4:8])
			if version > logVersion {
				return fmt.Errorf("flexfile: unsupported log version %d", version)
			}
			off = 16
			versioned = true
		}
	}
	if !versioned {
		// Legacy WAL written before the magic header and CRC fields were added.
		// Entries are 16 bytes (two uint64s) with no checksum.
		entrySize = 16
	}

	for ; off+entrySize <= totalSize; off += entrySize {
		p := (*uint64)(unsafe.Pointer(&fullBuf[off]))
		v1 := *p
		p = (*uint64)(unsafe.Pointer(&fullBuf[off+8]))
		v2 := *p

		if v1 == 0 && v2 == 0 {
			continue
		}

		// Verify CRC for versioned entries.
		if versioned {
			storedCRC := binary.BigEndian.Uint32(fullBuf[off+16 : off+20])
			c := crc32.NewIEEE()
			var b [16]byte
			binary.BigEndian.PutUint64(b[0:8], v1)
			binary.BigEndian.PutUint64(b[8:16], v2)
			c.Write(b[:])
			if c.Sum32() != storedCRC {
				// Corrupted entry: stop replaying here.
				// For WAL, truncation of the last partial/corrupt entry is acceptable.
				return ErrCorruptLog
			}
		}

		o := op(v1 & 0x3)
		p1 := (v1 >> 2) & 0xFFFFFFFFFFFF
		p2 := ((v1 >> 50) & 0x3FFF) | ((v2 & 0x3FFFFFFFFF) << 14)
		p3 := uint32(v2 >> 34)

		switch o {
		case opTreeInsert:
			ff.tree.InsertWithTag(p1, p2, p3, 0)
		case opTreeCollapseN:
			ff.tree.Delete(p1, uint64(p2))
		case opSetTag:
			ff.tree.SetTag(p1, uint16(p2))
		case opGC:
			ff.tree.UpdatePoff(p1, p2, p3)
		}
	}

	// Without this, a second crash would lose the replayed operations.
	treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
	treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
	if err := ff.tree.Sync(treeMetaPath, treeNodePath); err != nil {
		return fmt.Errorf("failed to sync tree after redo: %w", err)
	}

	if err := ff.logFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate log after redo: %w", err)
	}
	ff.logTotalSize = 0
	return nil
}

// Close flushes all pending data and releases resources.
func (ff *FlexFile) Close() error {
	syncErr := ff.Sync()
	// Always sync tree on close so crash recovery finds a valid tree + empty log.
	if ff.logTotalSize > 0 {
		treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
		treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
		ff.tree.Sync(treeMetaPath, treeNodePath)
		_ = ff.logFile.Truncate(0) // best-effort; Close() releases the fd regardless
	}
	_ = ff.bm.close() // already flushed by Sync(); ignore duplicate-flush error
	for i, chunk := range ff.chunks {
		if chunk != nil {
			syscall.Munmap(chunk)
			ff.chunks[i] = nil
		}
	}
	dataErr := ff.dataFile.Close()
	logErr := ff.logFile.Close()
	checksumErr := ff.checksumFile.Close()
	if syncErr != nil {
		return syncErr
	}
	if dataErr != nil {
		return dataErr
	}
	if logErr != nil {
		return logErr
	}
	return checksumErr
}

// Sync flushes pending data to disk.
// Uses fdatasync to match C flexfile_sync_r. The tree is checkpointed to disk
// once the WAL exceeds logMaxSize.
func (ff *FlexFile) Sync() error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	atomic.AddUint64(&syncCount, 1)

	if ff.bm.currentBuf.off > 0 {
		if err := ff.writeChecksums(ff.bm.currentBuf.blkID, ff.bm.currentBuf.data, ff.bm.currentBuf.off); err != nil {
			return err
		}
	}
	if err := ff.bm.flush(); err != nil {
		return err
	}
	// Use fdatasync (syscall 187 on macOS) — faster than F_FULLFSYNC and
	// sufficient for our durability guarantee (data already written via mmap).
	syscall.Syscall(syscall.SYS_FDATASYNC, ff.dataFile.Fd(), 0, 0)
	syscall.Syscall(syscall.SYS_FDATASYNC, ff.checksumFile.Fd(), 0, 0)
	if err := ff.syncLog(); err != nil {
		return err
	}

	if ff.logTotalSize >= logMaxSize {
		treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
		treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
		ff.tree.Sync(treeMetaPath, treeNodePath)
		_ = ff.logFile.Truncate(0) // best-effort; log will be replayed and re-truncated on next open
		ff.logTotalSize = 0
	}
	return nil
}

func (ff *FlexFile) syncLog() error {
	if ff.logTotalSize == 0 {
		var h [16]byte
		binary.BigEndian.PutUint32(h[0:4], logMagic)
		binary.BigEndian.PutUint32(h[4:8], logVersion)
		n, err := ff.logFile.WriteAt(h[:], 0)
		if err != nil {
			return fmt.Errorf("flexfile: log header write failed: %w", err)
		}
		ff.logTotalSize += uint64(n)
	}
	if ff.logBufSize == 0 {
		return nil
	}
	n, err := ff.logFile.WriteAt(ff.logBuf[:ff.logBufSize], int64(ff.logTotalSize))
	if err != nil {
		return fmt.Errorf("flexfile: log write failed: %w", err)
	}
	ff.logTotalSize += uint64(n)
	ff.logBufSize = 0
	syscall.Syscall(syscall.SYS_FDATASYNC, ff.logFile.Fd(), 0, 0)
	return nil
}

// flushLogIfFull writes the in-memory log buffer to disk when it is full.
// Unlike Sync(), this does NOT fdatasync the data file — matching C's behavior
// where log flushes are independent of data persistence.
func (ff *FlexFile) flushLogIfFull() error {
	if ff.logBufSize < logMemCap {
		return nil
	}
	return ff.syncLog()
}

func (ff *FlexFile) logWrite(o op, p1, p2 uint64, p3 uint32) {
	v1 := uint64(o) | (p1 << 2) | (p2 << 50)
	v2 := (p2 >> 14) | (uint64(p3) << 34)

	off := uintptr(ff.logBufSize)
	p := (*uint64)(unsafe.Add(ff.logBufBase, off))
	*p = v1
	p = (*uint64)(unsafe.Add(ff.logBufBase, off+8))
	*p = v2

	// Compute CRC32
	c := crc32.NewIEEE()
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], v1)
	binary.BigEndian.PutUint64(b[8:16], v2)
	c.Write(b[:])
	storedCRC := c.Sum32()

	p32 := (*uint32)(unsafe.Add(ff.logBufBase, off+16))
	*p32 = storedCRC

	ff.logBufSize += logEntrySize
	if ff.metrics.WALWriteCount != nil {
		ff.metrics.WALWriteCount.Add(1)
	}
}

// Read reads data from the flexfile at the given logical offset.
func (ff *FlexFile) Read(buf []byte, loff uint64) (int, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if loff+uint64(len(buf)) > ff.tree.MaxLoff() {
		return 0, io.EOF
	}

	var qbuf [16]flextree.QueryResult
	results := ff.tree.Query(loff, uint64(len(buf)), qbuf[:])
	bytesRead := 0
	for _, res := range results {
		slen := int(res.Len)
		targetBuf := buf[bytesRead : bytesRead+slen]

		n := ff.bm.read(targetBuf, res.Poff)
		if n == 0 {
			chunkIdx := res.Poff >> chunkBits
			chunkOff := res.Poff & (chunkSize - 1)

			var chunk []byte
			if chunkIdx == ff.lastChunkIdx {
				chunk = ff.lastChunk
			} else if chunkIdx < uint64(len(ff.chunks)) {
				chunk = ff.chunks[chunkIdx]
			}

			if chunk == nil {
				if err := ff.ensureMapping(res.Poff); err == nil {
					chunk = ff.lastChunk
				}
			}

			if chunk != nil {
				copy(targetBuf, chunk[chunkOff:chunkOff+res.Len])
				n = int(res.Len)

				// Verify checksum for the mapped data.
				if err := ff.verifyChecksum(res.Poff, targetBuf); err != nil {
					return bytesRead, err
				}
			}

			if n == 0 {
				var err error
				n, err = ff.dataFile.ReadAt(targetBuf, int64(res.Poff))
				if err != nil && err != io.EOF {
					return bytesRead, err
				}
				// Verify checksum for the data read from disk.
				if n > 0 {
					if err := ff.verifyChecksum(res.Poff, targetBuf[:n]); err != nil {
						return bytesRead, err
					}
				}
			}

		}
		bytesRead += n
	}

	return bytesRead, nil
}

// Insert inserts data at the given logical offset, shifting subsequent data.
func (ff *FlexFile) Insert(buf []byte, loff uint64) (int, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.insertR(buf, loff, true)
}

func (ff *FlexFile) insertR(buf []byte, loff uint64, commit bool) (int, error) {
	if loff > ff.tree.MaxLoff() {
		return 0, ErrNoHoles
	}

	bytesInserted := 0
	remaining := len(buf)
	for remaining > 0 {
		poff := ff.bm.offset()
		n, err := ff.bm.write(buf[bytesInserted:])
		if err != nil {
			return bytesInserted, err
		}
		if n == 0 {
			return bytesInserted, fmt.Errorf("block manager write failed")
		}

		err = ff.tree.InsertWithTag(loff+uint64(bytesInserted), poff, uint32(n), 0)
		if err != nil {
			return bytesInserted, err
		}

		ff.logWrite(opTreeInsert, loff+uint64(bytesInserted), poff, uint32(n))
		bytesInserted += n
		remaining -= n
	}

	if commit && ff.logBufSize >= logMemCap {
		if err := ff.flushLogIfFull(); err != nil {
			return bytesInserted, err
		}
	}

	return bytesInserted, nil
}

// Collapse removes a logical range and compacts following extents.
func (ff *FlexFile) Collapse(loff, length uint64) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.collapseR(loff, length, true)
}

func (ff *FlexFile) collapseR(loff, length uint64, commit bool) error {
	if loff+length > ff.tree.MaxLoff() {
		return ErrOutOfRange
	}

	var qbuf [16]flextree.QueryResult
	results := ff.tree.Query(loff, length, qbuf[:])
	err := ff.tree.Delete(loff, length)
	if err != nil {
		return err
	}

	ff.logWrite(opTreeCollapseN, loff, length, 0)

	for _, res := range results {
		blkID := res.Poff >> blockBits
		ff.bm.updateUsage(blkID, -int32(res.Len))
	}

	if commit && ff.logBufSize >= logMemCap {
		if err := ff.flushLogIfFull(); err != nil {
			return err
		}
	}

	return nil
}

// Write writes data at the given logical offset, replacing existing data.
// If loff is at the end of file, Write behaves like Insert.
func (ff *FlexFile) Write(buf []byte, loff uint64) (int, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	size := ff.tree.MaxLoff()

	if loff > size {
		return 0, ErrNoHoles
	} else if loff == size {
		return ff.insertR(buf, loff, true)
	}

	if loff+uint64(len(buf)) > size {
		err := ff.collapseR(loff, size-loff, true)
		if err != nil {
			return 0, err
		}
		return ff.insertR(buf, loff, true)
	}

	tag, _ := ff.getTagR(loff)

	err := ff.collapseR(loff, uint64(len(buf)), false)
	if err != nil {
		return 0, err
	}

	n, err := ff.insertR(buf, loff, false)
	if err != nil {
		return 0, err
	}

	if tag != 0 {
		ff.setTagR(loff, tag, false)
	}

	if ff.logBufSize >= logMemCap {
		if err := ff.flushLogIfFull(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Update replaces olen bytes at loff with the contents of buf.
func (ff *FlexFile) Update(buf []byte, loff, olen uint64) (int, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if loff+olen > ff.tree.MaxLoff() {
		return 0, ErrOutOfRange
	}

	tag, _ := ff.getTagR(loff)

	err := ff.collapseR(loff, olen, false)
	if err != nil {
		return 0, err
	}

	n, err := ff.insertR(buf, loff, false)
	if err != nil {
		return 0, err
	}

	if tag != 0 {
		ff.setTagR(loff, tag, false)
	}

	if ff.logBufSize >= logMemCap {
		if err := ff.flushLogIfFull(); err != nil {
			return n, err
		}
	}

	return n, nil
}

// SetTag sets a tag at the given logical offset.
func (ff *FlexFile) SetTag(loff uint64, tag uint16) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.setTagR(loff, tag, true)
}

func (ff *FlexFile) setTagR(loff uint64, tag uint16, commit bool) error {
	err := ff.tree.SetTag(loff, tag)
	if err != nil {
		return err
	}

	ff.logWrite(opSetTag, loff, uint64(tag), 0)

	if commit && ff.logBufSize >= logMemCap {
		if err := ff.flushLogIfFull(); err != nil {
			return err
		}
	}

	return nil
}

// GetTag retrieves the tag at the given logical offset.
func (ff *FlexFile) GetTag(loff uint64) (uint16, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.getTagR(loff)
}

func (ff *FlexFile) getTagR(loff uint64) (uint16, error) {
	return ff.tree.GetTag(loff)
}

func (ff *FlexFile) Size() uint64 {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.sizeR()
}

func (ff *FlexFile) sizeR() uint64 {
	return ff.tree.MaxLoff()
}

// Fallocate ensures space is allocated for size bytes starting at loff.
func (ff *FlexFile) Fallocate(loff, size uint64) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	remain := size

	off := uint64(0)
	buf := make([]byte, maxExtentSize)
	for remain > 0 {
		tsize := min(remain, uint64(maxExtentSize))
		_, err := ff.insertR(buf[:tsize], loff+off, true)
		if err != nil {
			return err
		}
		off += tsize
		remain -= tsize
	}
	return nil
}

// Ftruncate resizes the flexfile to size bytes, collapsing the tail if needed.
func (ff *FlexFile) Ftruncate(size uint64) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	fsize := ff.tree.MaxLoff()

	if fsize <= size {
		return nil
	}
	return ff.collapseR(size, fsize-size, true)
}

// GC reclaims physical space by rewriting extents on sparsely-used blocks.
func (ff *FlexFile) GC() {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if ff.bm.freeBlocks >= gcThreshold {
		return
	}

	threshold := uint32(blockSize / 2)
	ff.tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		blkID := poff >> blockBits
		if ff.bm.usage[blkID] < threshold {
			buf := make([]byte, length)
			// Internal read from within the same locked FlexFile.
			// We can't call ff.Read because it would deadlock.
			// We need a readR or similar.
			n, err := ff.readR(buf, loff)
			if err != nil || uint32(n) != length {
				return true
			}

			newPoff := ff.bm.offset()
			n, err = ff.bm.write(buf)
			if err != nil || uint32(n) != length {
				return true // GC is best-effort; skip extents that can't be rewritten
			}

			ff.tree.UpdatePoff(loff, newPoff, length)
			ff.bm.updateUsage(blkID, -int32(length))

			ff.logWrite(opGC, loff, newPoff, length)

			if ff.metrics.GCReclaimedBytes != nil {
				ff.metrics.GCReclaimedBytes.Add(uint64(length))
			}
			if ff.metrics.GCMovedBytes != nil {
				ff.metrics.GCMovedBytes.Add(uint64(length))
			}
		}

		return true
	})
}

func (ff *FlexFile) readR(buf []byte, loff uint64) (int, error) {
	if loff+uint64(len(buf)) > ff.tree.MaxLoff() {
		return 0, io.EOF
	}

	var qbuf [16]flextree.QueryResult
	results := ff.tree.Query(loff, uint64(len(buf)), qbuf[:])
	bytesRead := 0
	for _, res := range results {
		slen := int(res.Len)
		targetBuf := buf[bytesRead : bytesRead+slen]

		n := ff.bm.read(targetBuf, res.Poff)
		if n == 0 {
			chunkIdx := res.Poff >> chunkBits
			chunkOff := res.Poff & (chunkSize - 1)

			var chunk []byte
			if chunkIdx == ff.lastChunkIdx {
				chunk = ff.lastChunk
			} else if chunkIdx < uint64(len(ff.chunks)) {
				chunk = ff.chunks[chunkIdx]
			}

			if chunk == nil {
				if err := ff.ensureMapping(res.Poff); err == nil {
					chunk = ff.lastChunk
				}
			}

			if chunk != nil {
				copy(targetBuf, chunk[chunkOff:chunkOff+res.Len])
				n = int(res.Len)

				// Verify checksum for the mapped data.
				if err := ff.verifyChecksum(res.Poff, targetBuf); err != nil {
					return bytesRead, err
				}
			}

			if n == 0 {
				var err error
				n, err = ff.dataFile.ReadAt(targetBuf, int64(res.Poff))
				if err != nil && err != io.EOF {
					return bytesRead, err
				}
				// Verify checksum for the data read from disk.
				if n > 0 {
					if err := ff.verifyChecksum(res.Poff, targetBuf[:n]); err != nil {
						return bytesRead, err
					}
				}
			}
		}
		bytesRead += n
	}

	return bytesRead, nil
}

// IterateExtents calls fn for each extent in the logical range [start, end).
// fn receives the logical offset, the file tag, and the raw data bytes for
// that extent. Iteration stops early if fn returns false.
func (ff *FlexFile) IterateExtents(start, end uint64, fn func(loff uint64, tag uint16, data []byte) bool) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	buf := make([]byte, maxExtentSize)

	ff.tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		if loff >= end {
			return false
		}
		if loff < start {
			return true
		}
		n := int(length)
		if n > len(buf) {
			buf = make([]byte, n)
		}
		nn, err := ff.dataFile.ReadAt(buf[:n], int64(poff))
		if err != nil || nn != n {
			return true
		}
		return fn(loff, tag, buf[:n])
	})
}

// buffer represents an in-memory block buffer used by the block manager.
type buffer struct {
	blkID uint64
	off   uint64
	data  []byte
}

// blockManager manages physical block allocation and buffered writes.
type blockManager struct {
	ff             *FlexFile
	usage          []uint32
	freeBlocks     uint64
	freeBlocksHint uint64

	currentBuf *buffer
}

// newBlockManager creates a new block manager for ff.
func newBlockManager(ff *FlexFile) *blockManager {
	return &blockManager{
		ff:         ff,
		usage:      make([]uint32, 16),
		freeBlocks: 16,
		currentBuf: &buffer{data: make([]byte, blockSize)},
	}
}

func (bm *blockManager) close() error {
	return bm.flush()
}

// init initializes block-usage state by scanning the extent tree.
func (bm *blockManager) init(tree *flextree.Tree) error {
	tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		p := poff
		l := uint64(length)
		for l > 0 {
			blkID := p >> blockBits
			remain := blockSize - (p % blockSize)
			tlen := min(l, remain)
			bm.updateUsage(blkID, int32(tlen))
			p += tlen
			l -= tlen
		}
		return true
	})
	bm.freeBlocksHint = 0
	blkID, err := bm.findEmptyBlock()
	if err != nil {
		return err
	}
	bm.currentBuf.blkID = blkID
	bm.currentBuf.off = 0
	// Zero out the buffer for the new block to ensure clean checksums for partial pages.
	for i := range bm.currentBuf.data {
		bm.currentBuf.data[i] = 0
	}
	return nil
}

func (bm *blockManager) findEmptyBlock() (uint64, error) {
	for i := bm.freeBlocksHint; i < uint64(len(bm.usage)); i++ {
		if bm.usage[i] == 0 {
			bm.freeBlocksHint = i
			return i, nil
		}
	}
	for i := uint64(0); i < bm.freeBlocksHint; i++ {
		if bm.usage[i] == 0 {
			bm.freeBlocksHint = i
			return i, nil
		}
	}
	// Grow capacity if no free blocks found.
	blkID := uint64(len(bm.usage))
	bm.ensureUsageCapacity(blkID)
	bm.freeBlocksHint = blkID
	return blkID, nil
}

// updateUsage adjusts the byte-usage counter for blkID by delta.
func (bm *blockManager) updateUsage(blkID uint64, delta int32) {
	if delta == 0 {
		return
	}
	if blkID >= uint64(len(bm.usage)) {
		bm.ensureUsageCapacity(blkID)
	}
	old := bm.usage[blkID]
	newUsage := uint32(int32(old) + delta)
	bm.usage[blkID] = newUsage
	if old == 0 {
		bm.freeBlocks--
	} else if newUsage == 0 {
		bm.freeBlocks++
	}
}

func (bm *blockManager) ensureUsageCapacity(maxBlkID uint64) {
	if maxBlkID < uint64(len(bm.usage)) {
		return
	}
	newCap := uint64(len(bm.usage))
	if newCap == 0 {
		newCap = 16
	}
	for newCap <= maxBlkID {
		newCap *= 2
	}
	newUsage := make([]uint32, newCap)
	copy(newUsage, bm.usage)
	bm.freeBlocks += newCap - uint64(len(bm.usage))
	bm.usage = newUsage
}

// offset returns the current physical write offset.
func (bm *blockManager) offset() uint64 {
	return bm.currentBuf.blkID*blockSize + bm.currentBuf.off
}

// write copies up to one block's worth of buf into the current block buffer.
func (bm *blockManager) write(buf []byte) (int, error) {
	n := len(buf)
	remain := int(blockSize - bm.currentBuf.off)
	if n > remain {
		n = remain
	}

	copy(bm.currentBuf.data[bm.currentBuf.off:], buf[:n])

	if bm.usage[bm.currentBuf.blkID] == 0 {
		bm.freeBlocks--
	}
	bm.usage[bm.currentBuf.blkID] += uint32(n)
	bm.currentBuf.off += uint64(n)

	if bm.currentBuf.off == blockSize {
		if err := bm.nextBlock(); err != nil {
			return n, err
		}
	}

	return n, nil
}

// nextBlock flushes the current block synchronously and moves to the next one.
func (bm *blockManager) nextBlock() error {
	if bm.currentBuf.off > 0 {
		atomic.AddUint64(&writeCount, 1)
		_, err := bm.ff.dataFile.WriteAt(bm.currentBuf.data[:bm.currentBuf.off], int64(bm.currentBuf.blkID*blockSize))
		if err != nil {
			return fmt.Errorf("flexfile: block write failed: %w", err)
		}

		if err := bm.ff.writeChecksums(bm.currentBuf.blkID, bm.currentBuf.data, bm.currentBuf.off); err != nil {
			return err
		}
	}
	blkID, err := bm.findEmptyBlock()
	if err != nil {
		return err
	}
	bm.currentBuf.blkID = blkID
	bm.currentBuf.off = 0
	// Zero out the buffer for the new block to ensure clean checksums for partial pages.
	for i := range bm.currentBuf.data {
		bm.currentBuf.data[i] = 0
	}
	return nil
}

// flush ensures the current block buffer is written to disk.
func (bm *blockManager) flush() error {
	if bm.currentBuf.off > 0 {
		return bm.nextBlock()
	}
	return nil
}

// read attempts to serve buf from the current in-memory block buffer.
// Returns the number of bytes read, or 0 if poff is not in the current buffer.
func (bm *blockManager) read(buf []byte, poff uint64) int {
	blkID := poff / blockSize
	if blkID != bm.currentBuf.blkID {
		return 0
	}

	blkOff := poff % blockSize
	remain := blockSize - blkOff
	n := len(buf)
	if uint64(n) > remain {
		n = int(remain)
	}

	if blkOff >= bm.currentBuf.off {
		return 0
	}
	if blkOff+uint64(n) > bm.currentBuf.off {
		n = int(bm.currentBuf.off - blkOff)
	}

	copy(buf, bm.currentBuf.data[blkOff:blkOff+uint64(n)])
	return n
}

func (ff *FlexFile) verifyChecksum(poff uint64, data []byte) error {
	numPagesInBlock := uint64(blockSize / pageSize)
	offInBlock := poff % blockSize
	blkID := poff / blockSize

	// Skip verification for the active write block.
	if blkID == ff.bm.currentBuf.blkID {
		return nil
	}

	currentOff := uint64(0)

	for currentOff < uint64(len(data)) {
		pageIdx := (offInBlock + currentOff) / pageSize
		pageStart := pageIdx * pageSize

		storedCRCBuf := make([]byte, 4)
		checkOff := int64(blkID*numPagesInBlock*uint64(checksumSize) + pageIdx*uint64(checksumSize))
		if _, err := ff.checksumFile.ReadAt(storedCRCBuf, checkOff); err != nil {
			if err == io.EOF {
				return nil // No checksum yet
			}
			return err
		}

		storedCRC := binary.BigEndian.Uint32(storedCRCBuf)
		if storedCRC != 0 {
			// Get the full page for verification.
			fullPage := make([]byte, pageSize)
			chunkIdx := (blkID*blockSize + pageStart) >> chunkBits
			chunkOff := (blkID*blockSize + pageStart) & (chunkSize - 1)

			if chunkIdx < uint64(len(ff.chunks)) && ff.chunks[chunkIdx] != nil {
				chunk := ff.chunks[chunkIdx]
				copy(fullPage, chunk[chunkOff:])
			} else {
				// Fallback to ReadAt
				if _, err := ff.dataFile.ReadAt(fullPage, int64(blkID*blockSize+pageStart)); err != nil {
					if err == io.EOF {
						goto next // Sparse file / hole
					}
					return err
				}
			}

			if crc := crc32.ChecksumIEEE(fullPage); crc != storedCRC {
				if crc == 0 && storedCRC == 0xFFFFFFFF {
					// okay
				} else {
					// Check if page is all zeros (common for unwritten pages)
					allZero := true
					for _, b := range fullPage {
						if b != 0 {
							allZero = false
							break
						}
					}
					if allZero {
						goto next
					}
					return fmt.Errorf("%w: at poff %d, got %08x want %08x", ErrCorruptData, blkID*blockSize+pageStart, crc, storedCRC)
				}
			}

		}

	next:
		// Advance to next page boundary.

		bytesReadInThisPage := (offInBlock + currentOff) % pageSize
		bytesLeftInPage := pageSize - bytesReadInThisPage
		currentOff += min(bytesLeftInPage, uint64(len(data))-currentOff)
	}
	return nil
}

func (ff *FlexFile) writeChecksums(blkID uint64, data []byte, length uint64) error {
	numPagesInBlock := uint64(blockSize / pageSize)
	// We only write checksums for FULL pages.
	// Partial pages at the very end of the file will be checksummed once they are full.
	numPages := length / pageSize
	var b [4]byte
	for i := uint64(0); i < numPages; i++ {
		pageStart := i * pageSize
		pageEnd := (i + 1) * pageSize
		pageData := data[pageStart:pageEnd]
		c := crc32.ChecksumIEEE(pageData)
		if c == 0 {
			c = 0xFFFFFFFF // Sentinel for natural zero CRC
		}
		binary.BigEndian.PutUint32(b[:], c)

		checkOff := int64(blkID*numPagesInBlock*uint64(checksumSize) + i*uint64(checksumSize))
		if _, err := ff.checksumFile.WriteAt(b[:], checkOff); err != nil {
			return fmt.Errorf("flexfile: checksum write failed: %w", err)
		}
	}
	return nil
}

func (ff *FlexFile) ensureMapping(offset uint64) error {
	chunkIdx := offset >> chunkBits
	if chunkIdx == ff.lastChunkIdx {
		return nil
	}
	if chunkIdx < uint64(len(ff.chunks)) && ff.chunks[chunkIdx] != nil {
		ff.lastChunkIdx = chunkIdx
		ff.lastChunk = ff.chunks[chunkIdx]
		return nil
	}

	info, err := ff.dataFile.Stat()
	if err != nil {
		return err
	}
	currentSize := uint64(info.Size())
	if offset >= currentSize {
		return nil
	}

	requiredSize := int64((chunkIdx + 1) << chunkBits)
	if info.Size() < requiredSize {
		if err := ff.dataFile.Truncate(requiredSize); err != nil {
			return err
		}
	}

	ptr, err := syscall.Mmap(int(ff.dataFile.Fd()), int64(chunkIdx<<chunkBits), chunkSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	// Hint to the kernel that we intend to access this chunk sequentially and
	// that pages should be faulted in eagerly. Errors are advisory-only.
	p := uintptr(unsafe.Pointer(&ptr[0]))
	syscall.Syscall(syscall.SYS_MADVISE, p, uintptr(len(ptr)), syscall.MADV_SEQUENTIAL)
	syscall.Syscall(syscall.SYS_MADVISE, p, uintptr(len(ptr)), syscall.MADV_WILLNEED)

	if chunkIdx >= uint64(len(ff.chunks)) {
		newCap := uint64(len(ff.chunks))
		if newCap == 0 {
			newCap = 16
		}
		for newCap <= chunkIdx {
			newCap *= 2
		}
		newChunks := make([][]byte, newCap)
		copy(newChunks, ff.chunks)
		ff.chunks = newChunks
	}

	ff.chunks[chunkIdx] = ptr
	ff.lastChunkIdx = chunkIdx
	ff.lastChunk = ptr
	return nil
}
