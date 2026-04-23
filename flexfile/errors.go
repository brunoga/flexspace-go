package flexfile

import "errors"

var (
	// ErrNoHoles is returned when an operation would create a hole in the address space.
	ErrNoHoles = errors.New("flexfile: holes not allowed")

	// ErrOutOfRange is returned when an offset is beyond the current file size.
	ErrOutOfRange = errors.New("flexfile: out of range")

	// ErrCorruptData is returned when a checksum mismatch is detected.
	ErrCorruptData = errors.New("flexfile: data corruption detected")

	// ErrCorruptLog is returned when corruption is detected in the WAL.
	ErrCorruptLog = errors.New("flexfile: log corruption detected")

	// ErrStorageFull is returned when no free blocks are available.
	ErrStorageFull = errors.New("flexfile: storage capacity exceeded")
)
