package flexfile

import (
	"container/list"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/brunoga/flexspace-go/flextree"
)

var (
	syncCount uint64
)

const (
	// blockBits defines the block size exponent (block size = 4 MiB).
	blockBits = 22
	blockSize = 1 << blockBits

	// chunkBits defines the mmap chunk size exponent (chunk size = 64 MiB).
	chunkBits = 26
	chunkSize = 1 << chunkBits

	// logMemCap is the in-memory WAL buffer capacity (64 MiB).
	// Large enough to amortise fdatasync cost; entries are flushed on Sync()
	// or when the buffer fills, whichever comes first.
	logMemCap = 64 << 20
	// logMaxSize is the maximum on-disk WAL size before a tree checkpoint (2 GiB).
	logMaxSize = uint64(2 << 30)

	logMagic     = 0x464C4F47 // 'FLOG'
	logVersion   = 1
	logEntrySize = 24

	// gcThreshold is the minimum free-block count below which GC is triggered.
	gcThreshold = 64

	// maxMmapChunks caps the number of simultaneously mapped 64 MiB chunks to
	// bound virtual address space consumption. When exceeded, the least-recently-
	// used chunk is unmapped. 256 chunks = 16 GiB of virtual address space.
	maxMmapChunks = 256
)

type op uint8

const (
	opTreeInsert    op = 0
	opTreeCollapseN op = 1
	opGC            op = 2
	opSetTag        op = 3
)

const (
	maxExtentSize = 16384

	pageSize     = 4096
	checksumSize = 4 // CRC32 per page, stored in checksumFile
)

// FlexFile represents a flat logical address space backed by a memory-
// mapped data file, a write-ahead log (WAL), and a flextree extent map.
// It provides Insert, Collapse, Update, Read, SetTag, and IterateExtents.
type FlexFile struct {
	path string
	mu   sync.RWMutex

	dataFile     *os.File
	logFile      *os.File
	checksumFile *os.File

	tree *flextree.Tree

	logBuf     []byte
	logBufBase unsafe.Pointer
	logBufSize int

	logTotalSize uint64

	chunks     [][]byte
	mmapLRU    *list.List
	mmapLRUIdx map[uint64]*list.Element

	bm *blockManager

	metrics *Metrics
}

// SetMetrics associates performance counters with this FlexFile.
func (ff *FlexFile) SetMetrics(m Metrics) {
	ff.metrics = &m
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
	logInfo, err := logFile.Stat()
	if err != nil {
		dataFile.Close()
		logFile.Close()
		checksumFile.Close()
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}

	ff := &FlexFile{
		path:         path,
		tree:         tree,
		dataFile:     dataFile,
		logFile:      logFile,
		logTotalSize: uint64(logInfo.Size()),
		checksumFile: checksumFile,
		logBuf:       logBuf,
		logBufBase:   unsafe.Pointer(&logBuf[0]),
		chunks:       make([][]byte, 16),
		mmapLRU:      list.New(),
		mmapLRUIdx:   make(map[uint64]*list.Element),
		metrics:      &Metrics{},
	}

	if err := ff.redoLog(); err != nil {
		dataFile.Close()
		logFile.Close()
		checksumFile.Close()
		return nil, fmt.Errorf("failed to load tree: %w", err)
	}

	bm, err := newBlockManager(ff)
	if err != nil {
		ff.Close()
		return nil, fmt.Errorf("failed to initialize block manager: %w", err)
	}
	ff.bm = bm

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
	if totalSize >= 16 {
		magic := binary.BigEndian.Uint32(fullBuf[0:4])
		if magic == logMagic {
			version := binary.BigEndian.Uint32(fullBuf[4:8])
			if version > logVersion {
				return fmt.Errorf("flexfile: unsupported log version %d", version)
			}
			off = 16
		} else {
			// Legacy WAL written before the magic header was added.
			// Entries are 16 bytes (two uint64s) with no checksum.
			entrySize = 16
		}
	}

	numReplayed := 0
	for ; off+entrySize <= totalSize; off += entrySize {
		v1 := binary.LittleEndian.Uint64(fullBuf[off:])
		v2 := binary.LittleEndian.Uint64(fullBuf[off+8:])

		if v1 == 0 && v2 == 0 {
			continue
		}

		if entrySize == int64(logEntrySize) {
			storedCRC := binary.LittleEndian.Uint32(fullBuf[off+16 : off+20])
			c := crc32.NewIEEE()
			var b [16]byte
			binary.LittleEndian.PutUint64(b[0:8], v1)
			binary.LittleEndian.PutUint64(b[8:16], v2)
			c.Write(b[:])
			if c.Sum32() != storedCRC {
				slog.Warn("flexfile: log corruption detected, stopping redo", "offset", off)
				break
			}
		}

		o := op(v1 & 0x3)
		p1 := (v1 >> 2) & 0xFFFFFFFFFFFF
		p2 := ((v1 >> 50) & 0x3FFF) | ((v2 & 0x3FFFFFFFFF) << 14)
		p3 := uint32(v2 >> 34)

		switch o {
		case opTreeInsert:
			if err := ff.tree.InsertWithTag(p1, p2, p3, 0); err != nil {
				return fmt.Errorf("redoLog: InsertWithTag at offset %d failed: %w", p1, err)
			}
		case opTreeCollapseN:
			if err := ff.tree.Delete(p1, uint64(p2)); err != nil {
				return fmt.Errorf("redoLog: Delete at offset %d failed: %w", p1, err)
			}
		case opSetTag:
			if err := ff.tree.SetTag(p1, uint16(p2)); err != nil {
				return fmt.Errorf("redoLog: SetTag failed: %w", err)
			}
		case opGC:
			ff.tree.UpdatePoff(p1, p2, p3)
		}
		numReplayed++
	}

	if numReplayed > 0 {
		treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
		treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
		if err := ff.tree.Sync(treeMetaPath, treeNodePath); err != nil {
			return fmt.Errorf("failed to sync tree after redo: %w", err)
		}
	}

	if err := ff.logFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate log after redo: %w", err)
	}
	ff.logTotalSize = 0
	return nil
}

func (ff *FlexFile) Close() error {
	syncErr := ff.Sync()
	if ff.logTotalSize > 0 {
		treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
		treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
		if treeErr := ff.tree.Sync(treeMetaPath, treeNodePath); treeErr != nil && syncErr == nil {
			syncErr = treeErr
		}
		_ = ff.logFile.Truncate(0)
	}
	if ff.bm != nil {
		_ = ff.bm.close()
	}
	// Unmap all chunks — Read() and readR() store every mapping into ff.chunks,
	// so iterating the slice is sufficient to release all virtual address space.
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

func (ff *FlexFile) Sync() error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	atomic.AddUint64(&syncCount, 1)

	if ff.bm == nil {
		return nil
	}

	if ff.bm.currentBuf.off > 0 {
		if err := ff.writeChecksums(ff.bm.currentBuf.blkID, ff.bm.currentBuf.data, ff.bm.currentBuf.off); err != nil {
			return err
		}
	}
	if err := ff.bm.flush(); err != nil {
		return err
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, ff.dataFile.Fd(), 0, 0); errno != 0 {
		return fmt.Errorf("flexfile: data fdatasync failed: %w", errno)
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, ff.checksumFile.Fd(), 0, 0); errno != 0 {
		return fmt.Errorf("flexfile: checksum fdatasync failed: %w", errno)
	}
	if err := ff.syncLog(); err != nil {
		return err
	}

	// Checkpoint the tree and truncate the WAL once it exceeds logMaxSize.
	// Doing this on every Sync() would add a temp-write + rename to every call.
	if ff.logTotalSize >= logMaxSize {
		treeMetaPath := filepath.Join(ff.path, "FLEXTREE_META")
		treeNodePath := filepath.Join(ff.path, "FLEXTREE_NODE")
		if err := ff.tree.Sync(treeMetaPath, treeNodePath); err != nil {
			return err
		}
		_ = ff.logFile.Truncate(0)
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
		ff.logTotalSize = uint64(n)
		if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, ff.logFile.Fd(), 0, 0); errno != 0 {
			return fmt.Errorf("flexfile: log header sync failed: %w", errno)
		}
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
	if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, ff.logFile.Fd(), 0, 0); errno != 0 {
		return fmt.Errorf("flexfile: log sync failed: %w", errno)
	}
	return nil
}

func (ff *FlexFile) flushLogIfFull() error {
	if ff.logBufSize < logMemCap {
		return nil
	}
	return ff.syncLog()
}

func (ff *FlexFile) logWrite(o op, p1, p2 uint64, p3 uint32) {
	// Pre-flush if the buffer doesn't have room for one more entry.
	// logMemCap is not necessarily a multiple of logEntrySize, so the
	// "check after write" pattern used by callers leaves a small window
	// where logBufSize can sit in (logMemCap-logEntrySize, logMemCap) and
	// the next PutUint* write goes out of bounds.
	if ff.logBufSize+logEntrySize > logMemCap {
		if err := ff.syncLog(); err != nil {
			// Sync failed; discard buffered entries to prevent the overflow
			// panic. Durability is impaired but in-memory state is intact.
			// The error will surface the next time the caller calls Sync().
			ff.logBufSize = 0
		}
	}

	v1 := uint64(o) | (p1 << 2) | (p2 << 50)
	v2 := (p2 >> 14) | (uint64(p3) << 34)

	off := ff.logBufSize
	binary.LittleEndian.PutUint64(ff.logBuf[off:], v1)
	binary.LittleEndian.PutUint64(ff.logBuf[off+8:], v2)

	c := crc32.NewIEEE()
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], v1)
	binary.LittleEndian.PutUint64(b[8:16], v2)
	c.Write(b[:])
	storedCRC := c.Sum32()

	binary.LittleEndian.PutUint32(ff.logBuf[off+16:], storedCRC)

	ff.logBufSize += logEntrySize
	if ff.metrics.WALWriteCount != nil {
		ff.metrics.WALWriteCount.Add(1)
	}
}

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

		if res.IsHole() {
			for i := range targetBuf {
				targetBuf[i] = 0
			}
			bytesRead += slen
			continue
		}

		n := ff.bm.read(targetBuf, res.Poff)
		if n == 0 {
			chunkIdx := res.Poff >> chunkBits
			chunkOff := res.Poff & (chunkSize - 1)

			if chunkIdx >= uint64(len(ff.chunks)) {
				newChunks := make([][]byte, (chunkIdx+1)*2)
				copy(newChunks, ff.chunks)
				ff.chunks = newChunks
			}

			ptr := ff.chunks[chunkIdx]
			if ptr == nil {
				var err error
				ptr, err = syscall.Mmap(int(ff.dataFile.Fd()), int64(chunkIdx<<chunkBits), chunkSize, syscall.PROT_READ, syscall.MAP_SHARED)
				if err != nil {
					return bytesRead, err
				}
				ff.chunks[chunkIdx] = ptr
				elem := ff.mmapLRU.PushFront(chunkIdx)
				ff.mmapLRUIdx[chunkIdx] = elem
				if ff.mmapLRU.Len() > maxMmapChunks {
					oldest := ff.mmapLRU.Back()
					evictIdx := oldest.Value.(uint64)
					ff.mmapLRU.Remove(oldest)
					delete(ff.mmapLRUIdx, evictIdx)
					syscall.Munmap(ff.chunks[evictIdx])
					ff.chunks[evictIdx] = nil
				}
			} else if elem, ok := ff.mmapLRUIdx[chunkIdx]; ok {
				ff.mmapLRU.MoveToFront(elem)
			}
			copy(targetBuf, ptr[chunkOff:chunkOff+uint64(slen)])
			n = slen
			if err := ff.verifyChecksum(res.Poff, targetBuf[:n]); err != nil {
				return bytesRead, err
			}
		}
		bytesRead += n
	}

	return bytesRead, nil
}

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

func (ff *FlexFile) Collapse(loff, length uint64) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.collapseR(loff, length, true)
}

func (ff *FlexFile) collapseR(loff, length uint64, commit bool) error {
	if loff+length > ff.tree.MaxLoff() {
		return ErrOutOfRange
	}

	// Snapshot the physical extents before deletion so we can decrement block
	// usage. Query uses append internally, so qbuf is just a hint to avoid
	// heap allocation in the common case (most collapses span few extents).
	var qbuf [16]flextree.QueryResult
	extents := ff.tree.Query(loff, length, qbuf[:])

	if err := ff.tree.Delete(loff, length); err != nil {
		return err
	}
	ff.logWrite(opTreeCollapseN, loff, length, 0)

	for _, res := range extents {
		if !res.IsHole() {
			ff.bm.updateUsage(res.Poff>>blockBits, -int32(res.Len))
		}
	}

	if commit && ff.logBufSize >= logMemCap {
		return ff.flushLogIfFull()
	}
	return nil
}

func (ff *FlexFile) Update(buf []byte, loff, olen uint64) (int, error) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if loff+olen > ff.tree.MaxLoff() {
		return 0, ErrOutOfRange
	}

	tag, _ := ff.getTagR(loff)

	n, err := ff.insertR(buf, loff, false)
	if err != nil {
		return 0, err
	}

	if err := ff.collapseR(loff+uint64(n), olen, false); err != nil {
		_ = ff.collapseR(loff, uint64(n), false)
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

func (ff *FlexFile) SetTag(loff uint64, tag uint16) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.setTagR(loff, tag, true)
}

func (ff *FlexFile) setTagR(loff uint64, tag uint16, commit bool) error {
	if err := ff.tree.SetTag(loff, tag); err != nil {
		return err
	}
	ff.logWrite(opSetTag, loff, uint64(tag), 0)
	if commit && ff.logBufSize >= logMemCap {
		return ff.flushLogIfFull()
	}
	return nil
}

func (ff *FlexFile) GetTag(loff uint64) (uint16, error) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()
	return ff.getTagR(loff)
}

func (ff *FlexFile) getTagR(loff uint64) (uint16, error) {
	if loff >= ff.tree.MaxLoff() {
		return 0, ErrOutOfRange
	}
	tag, _ := ff.tree.GetTag(loff)
	return tag, nil
}

func (ff *FlexFile) Size() uint64 {
	ff.mu.RLock()
	defer ff.mu.RUnlock()
	return ff.tree.MaxLoff()
}

func (ff *FlexFile) IterateExtents(start, end uint64, fn func(loff uint64, tag uint16, data []byte) bool) error {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	buf := make([]byte, maxExtentSize)

	var iterErr error
	ff.tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		if loff >= end {
			return false
		}
		if loff < start {
			return true
		}
		if flextree.IsHole(poff) {
			return true
		}
		n := int(length)
		if n > len(buf) {
			buf = make([]byte, n)
		}
		// Check the block manager's in-memory buffer first: data may not
		// have been flushed to the data file yet if the block hasn't filled.
		if nr := ff.bm.read(buf[:n], poff); nr < n {
			nn, err := ff.dataFile.ReadAt(buf[:n], int64(poff))
			if err != nil || nn != n {
				// EOF or short read means the physical file is shorter than the
				// tree believes — expected after a crash. Stop iteration gracefully
				// rather than propagating an error that would abort recovery.
				if err == io.EOF || (err == nil && nn < n) {
					slog.Info("IterateExtents: graceful stop", "loff", loff, "poff", poff, "n", n, "nn", nn, "tag", tag, "err", err)
					return false
				}
				slog.Info("IterateExtents: read error", "loff", loff, "poff", poff, "n", n, "err", err)
				iterErr = err
				return false
			}
		}
		if !fn(loff, tag, buf[:n]) {
			return false
		}
		return true
	})
	return iterErr
}

type buffer struct {
	blkID uint64
	off   uint64
	data  []byte
}

type blockManager struct {
	ff             *FlexFile
	usage          []uint32
	freeBlocks     uint64
	freeBlocksHint uint64
	currentBuf     buffer
}

func newBlockManager(ff *FlexFile) (*blockManager, error) {
	info, err := ff.dataFile.Stat()
	if err != nil {
		return nil, err
	}
	nBlocks := (info.Size() + blockSize - 1) / blockSize
	if nBlocks < 16 {
		nBlocks = 16
	}
	usage := make([]uint32, nBlocks)
	bm := &blockManager{
		ff:         ff,
		usage:      usage,
		currentBuf: buffer{data: make([]byte, blockSize), blkID: ^uint64(0)},
	}
	if info.Size() > 0 {
		bm.rebuildUsage()
	}
	bm.pickNewBlock()
	return bm, nil
}

func (bm *blockManager) rebuildUsage() {
	bm.ff.tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		if flextree.IsHole(poff) {
			return true
		}
		blkID := poff / blockSize
		if blkID >= uint64(len(bm.usage)) {
			newUsage := make([]uint32, (blkID+1)*2)
			copy(newUsage, bm.usage)
			bm.usage = newUsage
		}
		bm.usage[blkID] += length
		return true
	})
	for _, u := range bm.usage {
		if u == 0 {
			bm.freeBlocks++
		}
	}
}

func (bm *blockManager) pickNewBlock() {
	for i := bm.freeBlocksHint; i < uint64(len(bm.usage)); i++ {
		if bm.usage[i] == 0 {
			bm.currentBuf.blkID = i
			bm.currentBuf.off = 0
			bm.freeBlocksHint = i + 1
			clear(bm.currentBuf.data)
			return
		}
	}
	oldLen := uint64(len(bm.usage))
	newUsage := make([]uint32, oldLen*2)
	copy(newUsage, bm.usage)
	bm.usage = newUsage
	bm.currentBuf.blkID = oldLen
	bm.currentBuf.off = 0
	bm.freeBlocksHint = oldLen + 1
	clear(bm.currentBuf.data)
}

func (bm *blockManager) write(buf []byte) (int, error) {
	blkOff := bm.currentBuf.off
	remain := uint64(blockSize) - blkOff
	n := len(buf)
	if uint64(n) > remain {
		n = int(remain)
	}

	copy(bm.currentBuf.data[blkOff:], buf[:n])
	bm.currentBuf.off += uint64(n)
	bm.usage[bm.currentBuf.blkID] += uint32(n)

	if bm.currentBuf.off == blockSize {
		if err := bm.flush(); err != nil {
			// Flush failed: roll back so the buffer is in a consistent state
			// for the next call. The bytes were never durably written.
			bm.currentBuf.off -= uint64(n)
			bm.usage[bm.currentBuf.blkID] -= uint32(n)
			return 0, err
		}
		bm.pickNewBlock()
	}
	return n, nil
}

func (bm *blockManager) read(buf []byte, poff uint64) int {
	blkID := poff / blockSize
	if blkID != bm.currentBuf.blkID {
		return 0
	}
	blkOff := poff % blockSize
	// Only serve bytes that have actually been written into the buffer.
	if blkOff >= bm.currentBuf.off {
		return 0
	}
	remain := bm.currentBuf.off - blkOff
	n := len(buf)
	if uint64(n) > remain {
		n = int(remain)
	}
	copy(buf[:n], bm.currentBuf.data[blkOff:blkOff+uint64(n)])
	return n
}

func (bm *blockManager) offset() uint64 {
	return bm.currentBuf.blkID*blockSize + bm.currentBuf.off
}

func (bm *blockManager) flush() error {
	if bm.currentBuf.blkID == ^uint64(0) || bm.currentBuf.off == 0 {
		return nil
	}
	if err := bm.ff.writeChecksums(bm.currentBuf.blkID, bm.currentBuf.data, bm.currentBuf.off); err != nil {
		return err
	}
	_, err := bm.ff.dataFile.WriteAt(bm.currentBuf.data[:bm.currentBuf.off], int64(bm.currentBuf.blkID*blockSize))
	return err
}

func (bm *blockManager) close() error {
	return bm.flush()
}

// updateUsage adjusts the byte-usage counter for blkID by delta and keeps freeBlocks in sync.
func (bm *blockManager) updateUsage(blkID uint64, delta int32) {
	if delta == 0 {
		return
	}
	if blkID >= uint64(len(bm.usage)) {
		newUsage := make([]uint32, (blkID+1)*2)
		copy(newUsage, bm.usage)
		bm.freeBlocks += uint64(len(newUsage) - len(bm.usage))
		bm.usage = newUsage
	}
	old := bm.usage[blkID]
	bm.usage[blkID] = uint32(int32(old) + delta)
	if old == 0 {
		bm.freeBlocks--
	} else if bm.usage[blkID] == 0 {
		bm.freeBlocks++
	}
}

func (ff *FlexFile) writeChecksums(blkID uint64, data []byte, length uint64) error {
	numPagesInBlock := uint64(blockSize / pageSize)
	// Only checksum full pages; partial trailing pages are checksummed once they fill.
	numPages := length / pageSize
	var b [4]byte
	for i := uint64(0); i < numPages; i++ {
		pageData := data[i*pageSize : (i+1)*pageSize]
		c := crc32.ChecksumIEEE(pageData)
		if c == 0 {
			c = 0xFFFFFFFF // sentinel for a naturally-zero CRC
		}
		binary.BigEndian.PutUint32(b[:], c)
		checkOff := int64(blkID*numPagesInBlock*uint64(checksumSize) + i*uint64(checksumSize))
		if _, err := ff.checksumFile.WriteAt(b[:], checkOff); err != nil {
			return fmt.Errorf("flexfile: checksum write failed: %w", err)
		}
	}
	return nil
}

func (ff *FlexFile) verifyChecksum(poff uint64, data []byte) error {
	numPagesInBlock := uint64(blockSize / pageSize)
	offInBlock := poff % blockSize
	blkID := poff / blockSize

	// Skip verification for the active write block (checksums not yet written).
	if blkID == ff.bm.currentBuf.blkID {
		return nil
	}

	currentOff := uint64(0)
	for currentOff < uint64(len(data)) {
		pageIdx := (offInBlock + currentOff) / pageSize
		pageStart := pageIdx * pageSize

		var storedCRCBuf [4]byte
		checkOff := int64(blkID*numPagesInBlock*uint64(checksumSize) + pageIdx*uint64(checksumSize))
		if _, err := ff.checksumFile.ReadAt(storedCRCBuf[:], checkOff); err != nil {
			if err == io.EOF {
				return nil // no checksum written yet
			}
			return err
		}
		storedCRC := binary.BigEndian.Uint32(storedCRCBuf[:])
		if storedCRC != 0 {
			fullPage := make([]byte, pageSize)
			chunkIdx := (blkID*blockSize + pageStart) >> chunkBits
			chunkOff := (blkID*blockSize + pageStart) & (chunkSize - 1)
			if chunkIdx < uint64(len(ff.chunks)) && ff.chunks[chunkIdx] != nil {
				copy(fullPage, ff.chunks[chunkIdx][chunkOff:])
			} else {
				if _, err := ff.dataFile.ReadAt(fullPage, int64(blkID*blockSize+pageStart)); err != nil {
					if err == io.EOF {
						goto next
					}
					return err
				}
			}
			if crc := crc32.ChecksumIEEE(fullPage); crc != storedCRC {
				if crc == 0 && storedCRC == 0xFFFFFFFF {
					// natural zero CRC matches sentinel — OK
				} else {
					allZero := true
					for _, b := range fullPage {
						if b != 0 {
							allZero = false
							break
						}
					}
					if !allZero {
						return fmt.Errorf("%w: at poff %d, got %08x want %08x",
							ErrCorruptData, blkID*blockSize+pageStart, crc, storedCRC)
					}
				}
			}
		}
	next:
		bytesReadInThisPage := (offInBlock + currentOff) % pageSize
		bytesLeftInPage := pageSize - bytesReadInThisPage
		currentOff += min(bytesLeftInPage, uint64(len(data))-currentOff)
	}
	return nil
}

// readR reads len(buf) bytes from logical offset loff without acquiring ff.mu.
// Must be called with ff.mu already held.
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
		if res.IsHole() {
			clear(targetBuf)
			bytesRead += slen
			continue
		}
		n := ff.bm.read(targetBuf, res.Poff)
		if n == 0 {
			chunkIdx := res.Poff >> chunkBits
			chunkOff := res.Poff & (chunkSize - 1)
			if chunkIdx >= uint64(len(ff.chunks)) {
				newChunks := make([][]byte, (chunkIdx+1)*2)
				copy(newChunks, ff.chunks)
				ff.chunks = newChunks
			}
			ptr := ff.chunks[chunkIdx]
			if ptr == nil {
				var err error
				ptr, err = syscall.Mmap(int(ff.dataFile.Fd()), int64(chunkIdx<<chunkBits), chunkSize, syscall.PROT_READ, syscall.MAP_SHARED)
				if err != nil {
					return bytesRead, err
				}
				ff.chunks[chunkIdx] = ptr
				elem := ff.mmapLRU.PushFront(chunkIdx)
				ff.mmapLRUIdx[chunkIdx] = elem
				if ff.mmapLRU.Len() > maxMmapChunks {
					oldest := ff.mmapLRU.Back()
					evictIdx := oldest.Value.(uint64)
					ff.mmapLRU.Remove(oldest)
					delete(ff.mmapLRUIdx, evictIdx)
					syscall.Munmap(ff.chunks[evictIdx])
					ff.chunks[evictIdx] = nil
				}
			} else if elem, ok := ff.mmapLRUIdx[chunkIdx]; ok {
				ff.mmapLRU.MoveToFront(elem)
			}
			copy(targetBuf, ptr[chunkOff:chunkOff+uint64(slen)])
			n = slen
		}
		bytesRead += n
	}
	return bytesRead, nil
}

// GC reclaims physical space by rewriting extents from sparsely-used blocks.
// It is a no-op when there are enough free blocks.
func (ff *FlexFile) GC() {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if ff.bm.freeBlocks >= gcThreshold {
		return
	}
	threshold := uint32(blockSize / 2)
	ff.tree.IterateExtents(func(loff, poff uint64, length uint32, tag uint16) bool {
		if flextree.IsHole(poff) {
			return true
		}
		blkID := poff >> blockBits
		if blkID >= uint64(len(ff.bm.usage)) || ff.bm.usage[blkID] >= threshold {
			return true
		}
		buf := make([]byte, length)
		n, err := ff.readR(buf, loff)
		if err != nil || uint32(n) != length {
			return true
		}
		newPoff := ff.bm.offset()
		n, err = ff.bm.write(buf)
		if err != nil || uint32(n) != length {
			return true // GC is best-effort; skip extents that can't be rewritten
		}
		if err := ff.tree.UpdatePoff(loff, newPoff, length); err != nil {
			return true
		}
		ff.bm.updateUsage(blkID, -int32(length))
		ff.logWrite(opGC, loff, newPoff, length)
		if ff.metrics.GCReclaimedBytes != nil {
			ff.metrics.GCReclaimedBytes.Add(uint64(length))
		}
		if ff.metrics.GCMovedBytes != nil {
			ff.metrics.GCMovedBytes.Add(uint64(length))
		}
		return true
	})
}
