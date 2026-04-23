package flexdb

import "errors"

var (
	// ErrInvalidKV is returned when a key is empty or exceeds MaxKVSize.
	ErrInvalidKV = errors.New("flexdb: invalid KV (empty key or key exceeds MaxKVSize)")

	// ErrBlobTooLarge is returned when a value exceeds MaxBlobSize.
	ErrBlobTooLarge = errors.New("flexdb: value exceeds MaxBlobSize")

	// ErrTableNotFound is returned when a table does not exist.
	ErrTableNotFound = errors.New("flexdb: table not found")

	// ErrCorruptWAL is returned when corruption is detected during WAL replay.
	ErrCorruptWAL = errors.New("flexdb: WAL corruption detected")

	// ErrCorruptData is returned when a checksum mismatch is detected during read.
	ErrCorruptData = errors.New("flexdb: data corruption detected")

	// ErrStorageFull is returned when no free blocks are available.
	ErrStorageFull = errors.New("flexdb: storage capacity exceeded")
)
