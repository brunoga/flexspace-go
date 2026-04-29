package flexdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	// blobFileMagic is the 4-byte file header for the BLOBFILE.
	blobFileMagic = uint32(0x46424C4F) // 'FBLO'

	// blobFileHeaderSize is the size of the BLOBFILE header.
	blobFileHeaderSize = 4

	// blobMagic is the 4-byte sentinel header identifying a blob reference value.
	// A value is a blob reference if and only if it is exactly blobSentinelSize
	// bytes and its first four bytes equal this constant.
	blobMagic = uint32(0xB10BB10B)

	// blobSentinelSize is the exact byte length of a blob reference value:
	// [magic:4][offset:8][size:4] = 16 bytes.
	blobSentinelSize = 4 + 8 + 4

	// MaxBlobSize is the maximum allowed size of a single blob value (1 GiB).
	MaxBlobSize = 1 << 30
)

// blobStore is an append-only flat file that stores large values for a single
// Table. Concurrent writers are serialised by mu; readers use pread (ReadAt),
// which is safe without a lock once the bytes have been fdatasynced.
type blobStore struct {
	mu   sync.Mutex
	f    *os.File
	size int64 // next write offset; owned by mu
}

// openBlobStore opens (or creates) the blob file at dir/BLOBFILE.
func openBlobStore(dir string) (*blobStore, error) {
	path := filepath.Join(dir, "BLOBFILE")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {

		return nil, fmt.Errorf("open blob file: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	size := info.Size()
	if size == 0 {
		var h [blobFileHeaderSize]byte
		binary.LittleEndian.PutUint32(h[:], blobFileMagic)
		if _, err := f.Write(h[:]); err != nil {
			f.Close()
			return nil, fmt.Errorf("write blob header: %w", err)
		}
		size = blobFileHeaderSize
	}

	return &blobStore{f: f, size: size}, nil
}

// write appends data to the blob file and returns the offset at which it was
// written. It calls fdatasync before returning so callers may safely store the
// returned offset in the memtable WAL: if the sentinel reaches durable storage,
// the blob bytes are guaranteed to already be on disk.
func (bs *blobStore) write(data []byte) (uint64, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	offset := bs.size
	if _, err := bs.f.WriteAt(data, offset); err != nil {
		return 0, fmt.Errorf("blob write at %d: %w", offset, err)
	}
	// Barrier: blob bytes must reach disk before the caller writes the sentinel
	// to the memtable WAL. Without this, a crash between WAL write and data
	// sync would leave a dangling sentinel pointing at unwritten blob bytes.
	if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, bs.f.Fd(), 0, 0); errno != 0 {
		return 0, fmt.Errorf("blob fdatasync: %w", errno)
	}
	bs.size += int64(len(data))
	return uint64(offset), nil
}

// read reads size bytes at offset from the blob file and returns them in a
// freshly allocated slice. Safe to call concurrently with write (pread).
func (bs *blobStore) read(offset uint64, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := bs.f.ReadAt(buf, int64(offset)); err != nil {
		return nil, fmt.Errorf("blob read at %d: %w", offset, err)
	}
	return buf, nil
}

func (bs *blobStore) sync() error {
	if _, _, errno := syscall.Syscall(syscall.SYS_FDATASYNC, bs.f.Fd(), 0, 0); errno != 0 {
		return fmt.Errorf("blob fdatasync: %w", errno)
	}
	return nil
}

func (bs *blobStore) close() error {
	syncErr := bs.sync()
	closeErr := bs.f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// isBlobSentinel reports whether val matches the blob reference sentinel
// pattern: exactly blobSentinelSize bytes whose first four bytes equal
// blobMagic. The Put/Update/Batch paths force any user value that matches
// this pattern through the blob store (even if it would otherwise fit
// inline), so no false positives can arise during reads.
func isBlobSentinel(val []byte) bool {
	return len(val) == blobSentinelSize &&
		binary.LittleEndian.Uint32(val) == blobMagic
}

// encodeBlobSentinel encodes a blob reference into buf.
// buf must be at least blobSentinelSize bytes long.
func encodeBlobSentinel(buf []byte, offset uint64, size uint32) {
	binary.LittleEndian.PutUint32(buf[0:4], blobMagic)
	binary.LittleEndian.PutUint64(buf[4:12], offset)
	binary.LittleEndian.PutUint32(buf[12:16], size)
}

// decodeBlobSentinel decodes a blob sentinel, returning offset and size.
// Callers must first verify isBlobSentinel(val).
func decodeBlobSentinel(val []byte) (offset uint64, size uint32) {
	return binary.LittleEndian.Uint64(val[4:12]),
		binary.LittleEndian.Uint32(val[12:16])
}
