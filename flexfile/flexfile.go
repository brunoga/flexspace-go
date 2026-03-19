// Package flexfile implements a flat logical address space backed by a memory-
// mapped data file, a write-ahead log (WAL), and a flextree extent map.
// It provides Insert, Collapse, Update, Read, SetTag, and GC operations that
// translate logical offsets to physical file positions, maintaining durability
// through the WAL. Not safe for concurrent use; callers must synchronise.
package flexfile

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	// maxOffset is the address space flexfile manages (800 GB).
	maxOffset = 800 << 30

	// blockBits defines the block size exponent (block size = 4 MiB).
	blockBits = 22
	// blockSize is the physical block size in bytes (4 MiB).
	blockSize = 1 << blockBits
	// blockCount is the total number of physical blocks in the address space.
	blockCount = maxOffset >> blockBits

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
	// chunkCount is the number of mmap chunks covering the address space.
	chunkCount = maxOffset >> chunkBits
)

// op represents a flexfile WAL operation code.
type op uint8

const (
	opTreeInsert    op = 0
	opTreeCollapseN op = 1
	opGC            op = 2
	opSetTag        op = 3
)

const logEntrySize = 16

// FlexFile is the Go implementation of the flexfile storage layer.
// It provides a logical address space backed by a flat data file, a WAL log,
// and a flextree extent map. All operations are not safe for concurrent use;
// callers must synchronise externally.
type FlexFile struct {
	path     string
	tree     *flextree.Tree
	dataFile *os.File
	logFile  *os.File

	logBuf       []byte
	logBufBase   unsafe.Pointer
	logBufSize   uint32
	logTotalSize uint64

	chunks map[uint64][]byte

	bm *blockManager
}

// Open opens or creates a flexfile at the given path.
func Open(path string) (*FlexFile, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	dataFilePath := filepath.Join(path, "DATA")
	dataFile, err := os.OpenFile(dataFilePath, os.O_RDWR|os.O_CREATE, 0644)
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
	logFile, err := os.OpenFile(logFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		dataFile.Close()
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	logBuf := make([]byte, logMemCap)
	ff := &FlexFile{
		path:       path,
		tree:       tree,
		dataFile:   dataFile,
		logFile:    logFile,
		logBuf:     logBuf,
		logBufBase: unsafe.Pointer(&logBuf[0]),
		chunks:     make(map[uint64][]byte),
	}

	// Redo log
	if err := ff.redoLog(); err != nil {
		ff.Close()
		return nil, fmt.Errorf("redo log failed: %w", err)
	}

	ff.bm = newBlockManager(ff)
	ff.bm.init(tree)

	return ff, nil
}

func (ff *FlexFile) ensureMapping(offset uint64) error {
	chunkIdx := offset >> chunkBits
	if chunkIdx >= chunkCount {
		return fmt.Errorf("offset out of bounds: %d", offset)
	}
	if _, ok := ff.chunks[chunkIdx]; ok {
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

	ff.chunks[chunkIdx] = ptr
	return nil
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

	for off := int64(0); off+logEntrySize <= totalSize; off += logEntrySize {
		p := (*uint64)(unsafe.Pointer(&fullBuf[off]))
		v1 := *p
		p = (*uint64)(unsafe.Pointer(&fullBuf[off+8]))
		v2 := *p

		if v1 == 0 && v2 == 0 {
			continue
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

	// Persist the recovered tree state before truncating the log.
	// Without this, a second crash would lose the replayed operations.
	treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
	treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
	if err := ff.tree.Sync(treeMetaPath, treeNodePath); err != nil {
		return fmt.Errorf("failed to sync tree after redo: %w", err)
	}

	ff.logFile.Truncate(0)
	ff.logTotalSize = 0
	return nil
}

// Close flushes all pending data and releases resources.
func (ff *FlexFile) Close() error {
	ff.Sync()
	// Always sync tree on close so crash recovery finds a valid tree + empty log.
	if ff.logTotalSize > 0 {
		treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
		treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
		ff.tree.Sync(treeMetaPath, treeNodePath)
		ff.logFile.Truncate(0)
	}
	ff.bm.close()
	for i, chunk := range ff.chunks {
		syscall.Munmap(chunk)
		delete(ff.chunks, i)
	}
	if err := ff.dataFile.Close(); err != nil {
		return err
	}
	if err := ff.logFile.Close(); err != nil {
		return err
	}
	return nil
}

// Sync flushes pending data to disk.
// Uses fdatasync to match C flexfile_sync_r. The tree is checkpointed to disk
// once the WAL exceeds logMaxSize.
func (ff *FlexFile) Sync() {
	atomic.AddUint64(&syncCount, 1)
	ff.bm.flush()
	// Use fdatasync (syscall 187 on macOS) — faster than F_FULLFSYNC and
	// sufficient for our durability guarantee (data already written via mmap).
	syscall.Syscall(syscall.SYS_FDATASYNC, ff.dataFile.Fd(), 0, 0)
	ff.syncLog()

	if ff.logTotalSize >= logMaxSize {
		treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
		treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
		ff.tree.Sync(treeMetaPath, treeNodePath)
		ff.logFile.Truncate(0)
		ff.logTotalSize = 0
	}
}

func (ff *FlexFile) syncLog() {
	if ff.logBufSize == 0 {
		return
	}
	n, err := ff.logFile.WriteAt(ff.logBuf[:ff.logBufSize], int64(ff.logTotalSize))
	if err != nil {
		panic(fmt.Sprintf("log write failed: %v", err))
	}
	ff.logTotalSize += uint64(n)
	ff.logBufSize = 0
	syscall.Syscall(syscall.SYS_FDATASYNC, ff.logFile.Fd(), 0, 0)
}

// flushLogIfFull writes the in-memory log buffer to disk when it is full.
// Unlike Sync(), this does NOT fdatasync the data file — matching C's behavior
// where log flushes are independent of data persistence.
func (ff *FlexFile) flushLogIfFull() {
	if ff.logBufSize < logMemCap {
		return
	}
	ff.syncLog()
}

func (ff *FlexFile) logWrite(o op, p1, p2 uint64, p3 uint32) {
	v1 := uint64(o) | (p1 << 2) | (p2 << 50)
	v2 := (p2 >> 14) | (uint64(p3) << 34)

	off := uintptr(ff.logBufSize)
	p := (*uint64)(unsafe.Add(ff.logBufBase, off))
	*p = v1
	p = (*uint64)(unsafe.Add(ff.logBufBase, off+8))
	*p = v2

	ff.logBufSize += logEntrySize
}

// Read reads data from the flexfile at the given logical offset.
func (ff *FlexFile) Read(buf []byte, loff uint64) (int, error) {
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

			if chunkIdx < chunkCount {
				chunk, ok := ff.chunks[chunkIdx]
				if !ok {
					ff.ensureMapping(res.Poff)
					chunk = ff.chunks[chunkIdx]
				}
				if chunk != nil {
					copy(targetBuf, chunk[chunkOff:chunkOff+res.Len])
					n = int(res.Len)
				}
			}

			if n == 0 {
				var err error
				n, err = ff.dataFile.ReadAt(targetBuf, int64(res.Poff))
				if err != nil && err != io.EOF {
					return bytesRead, err
				}
			}
		}
		bytesRead += n
	}

	return bytesRead, nil
}

// Insert inserts data at the given logical offset, shifting subsequent data.
func (ff *FlexFile) Insert(buf []byte, loff uint64) (int, error) {
	if loff > ff.tree.MaxLoff() {
		return 0, fmt.Errorf("holes not allowed")
	}

	// Hot path for single-extent insertions at the end.
	maxLoff := ff.tree.MaxLoff()
	if loff == maxLoff && len(buf) <= maxExtentSize {
		bm := ff.bm
		cb := bm.currentBuf
		// If the data won't fit in the remaining space of the current block,
		// advance to a fresh block so this write is always a single extent.
		if int(blockSize-cb.off) < len(buf) {
			bm.nextBlock()
			cb = bm.currentBuf
		}
		poff := cb.blkID*blockSize + cb.off
		n := len(buf)
		copy(cb.data[cb.off:], buf)
		if bm.usage[cb.blkID] == 0 {
			bm.freeBlocks--
		}
		bm.usage[cb.blkID] += uint32(n)
		cb.off += uint64(n)
		if cb.off == blockSize {
			bm.nextBlock()
		}

		err := ff.tree.InsertAppend(poff, uint32(n))
		if err != nil {
			return 0, err
		}

		// Fast log write with pre-calculated values.
		v1 := uint64(opTreeInsert) | (loff << 2) | (poff << 50)
		v2 := (poff >> 14) | (uint64(n) << 34)

		off := uintptr(ff.logBufSize)
		p := (*uint64)(unsafe.Add(ff.logBufBase, off))
		*p = v1
		p = (*uint64)(unsafe.Add(ff.logBufBase, off+8))
		*p = v2
		ff.logBufSize += logEntrySize

		if ff.logBufSize >= logMemCap {
			ff.flushLogIfFull()
		}
		return n, nil
	}

	return ff.insertR(buf, loff, true)
}

func (ff *FlexFile) insertR(buf []byte, loff uint64, commit bool) (int, error) {
	if loff > ff.tree.MaxLoff() {
		return 0, fmt.Errorf("holes not allowed")
	}

	bytesInserted := 0
	remaining := len(buf)
	for remaining > 0 {
		poff := ff.bm.offset()
		n := ff.bm.write(buf[bytesInserted:])
		if n == 0 {
			return bytesInserted, fmt.Errorf("block manager write failed")
		}

		err := ff.tree.InsertWithTag(loff+uint64(bytesInserted), poff, uint32(n), 0)
		if err != nil {
			return bytesInserted, err
		}

		ff.logWrite(opTreeInsert, loff+uint64(bytesInserted), poff, uint32(n))
		bytesInserted += n
		remaining -= n
	}

	if commit && ff.logBufSize >= logMemCap {
		ff.flushLogIfFull()
	}

	return bytesInserted, nil
}

// Collapse removes a logical range and compacts following extents.
func (ff *FlexFile) Collapse(loff, length uint64) error {
	return ff.collapseR(loff, length, true)
}

func (ff *FlexFile) collapseR(loff, length uint64, commit bool) error {
	if loff+length > ff.tree.MaxLoff() {
		return fmt.Errorf("out of range")
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
		ff.flushLogIfFull()
	}

	return nil
}

// Write writes data at the given logical offset, replacing existing data.
// If loff is at the end of file, Write behaves like Insert.
func (ff *FlexFile) Write(buf []byte, loff uint64) (int, error) {
	size := ff.tree.MaxLoff()
	if loff > size {
		return 0, fmt.Errorf("holes not allowed")
	} else if loff == size {
		return ff.Insert(buf, loff)
	}

	if loff+uint64(len(buf)) > size {
		err := ff.Collapse(loff, size-loff)
		if err != nil {
			return 0, err
		}
		return ff.Insert(buf, loff)
	}

	return ff.Update(buf, loff, uint64(len(buf)))
}

// Update replaces olen bytes at loff with the contents of buf.
func (ff *FlexFile) Update(buf []byte, loff, olen uint64) (int, error) {
	if loff+olen > ff.tree.MaxLoff() {
		return 0, fmt.Errorf("out of range")
	}

	tag, _ := ff.GetTag(loff)

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
		ff.flushLogIfFull()
	}

	return n, nil
}

// SetTag sets a tag at the given logical offset.
func (ff *FlexFile) SetTag(loff uint64, tag uint16) error {
	return ff.setTagR(loff, tag, true)
}

func (ff *FlexFile) setTagR(loff uint64, tag uint16, commit bool) error {
	err := ff.tree.SetTag(loff, tag)
	if err != nil {
		return err
	}

	ff.logWrite(opSetTag, loff, uint64(tag), 0)

	if commit && ff.logBufSize >= logMemCap {
		ff.flushLogIfFull()
	}

	return nil
}

// GetTag retrieves the tag at the given logical offset.
func (ff *FlexFile) GetTag(loff uint64) (uint16, error) {
	return ff.tree.GetTag(loff)
}

// Size returns the current logical size of the flexfile in bytes.
func (ff *FlexFile) Size() uint64 {
	return ff.tree.MaxLoff()
}

// Fallocate ensures space is allocated for size bytes starting at loff.
func (ff *FlexFile) Fallocate(loff, size uint64) error {
	remain := size
	off := uint64(0)
	buf := make([]byte, maxExtentSize)
	for remain > 0 {
		tsize := min(remain, uint64(maxExtentSize))
		_, err := ff.Insert(buf[:tsize], loff+off)
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
	fsize := ff.Size()
	if fsize <= size {
		return nil
	}
	return ff.Collapse(size, fsize-size)
}

// GC reclaims physical space by rewriting extents on sparsely-used blocks.
func (ff *FlexFile) GC() {
	if ff.bm.freeBlocks >= gcThreshold {
		return
	}

	threshold := uint32(blockSize / 2)
	ff.tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		blkID := poff >> blockBits
		if ff.bm.usage[blkID] < threshold {
			buf := make([]byte, length)
			n, err := ff.Read(buf, loff)
			if err != nil || uint32(n) != length {
				return true
			}

			newPoff := ff.bm.offset()
			n = ff.bm.write(buf)
			if uint32(n) != length {
				return true
			}

			ff.tree.UpdatePoff(loff, newPoff, length)
			ff.bm.updateUsage(blkID, -int32(length))

			ff.logWrite(opGC, loff, newPoff, length)
		}
		return true
	})
}

// IterateExtents calls fn for each extent in the logical range [start, end).
// fn receives the logical offset, the file tag, and the raw data bytes for
// that extent. Iteration stops early if fn returns false.
func (ff *FlexFile) IterateExtents(start, end uint64, fn func(loff uint64, tag uint16, data []byte) bool) {
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
		usage:      make([]uint32, blockCount),
		freeBlocks: blockCount,
		currentBuf: &buffer{data: make([]byte, blockSize)},
	}
}

func (bm *blockManager) close() {
	bm.flush()
}

// init initializes block-usage state by scanning the extent tree.
func (bm *blockManager) init(tree *flextree.Tree) {
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
	bm.currentBuf.blkID = bm.findEmptyBlock()
	bm.currentBuf.off = 0
}

func (bm *blockManager) findEmptyBlock() uint64 {
	for i := bm.freeBlocksHint; i < blockCount; i++ {
		if bm.usage[i] == 0 {
			bm.freeBlocksHint = i
			return i
		}
	}
	for i := uint64(0); i < bm.freeBlocksHint; i++ {
		if bm.usage[i] == 0 {
			bm.freeBlocksHint = i
			return i
		}
	}
	panic("no empty blocks")
}

// updateUsage adjusts the byte-usage counter for blkID by delta.
func (bm *blockManager) updateUsage(blkID uint64, delta int32) {
	if delta == 0 {
		return
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

// offset returns the current physical write offset.
func (bm *blockManager) offset() uint64 {
	return bm.currentBuf.blkID*blockSize + bm.currentBuf.off
}

// write copies up to one block's worth of buf into the current block buffer.
func (bm *blockManager) write(buf []byte) int {
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
		bm.nextBlock()
	}

	return n
}

// nextBlock flushes the current block synchronously and moves to the next one.
func (bm *blockManager) nextBlock() {
	if bm.currentBuf.off > 0 {
		atomic.AddUint64(&writeCount, 1)
		_, err := bm.ff.dataFile.WriteAt(bm.currentBuf.data[:bm.currentBuf.off], int64(bm.currentBuf.blkID*blockSize))
		if err != nil {
			panic(fmt.Sprintf("block write failed: %v", err))
		}
	}
	bm.currentBuf.blkID = bm.findEmptyBlock()
	bm.currentBuf.off = 0
}

// flush ensures the current block buffer is written to disk.
func (bm *blockManager) flush() {
	if bm.currentBuf.off > 0 {
		bm.nextBlock()
	}
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
