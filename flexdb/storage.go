package flexdb

import "github.com/brunoga/flexspace-go/flexfile"

// Storage is the pluggable persistence layer for a Table. It abstracts the
// ordered, tagged extent store that flexfile provides today. A future
// distributed backend (remote block store, replicated log, etc.) only needs
// to satisfy this interface; the rest of flexdb is unaware of the concrete
// type.
//
// flexfile.FlexFile satisfies Storage; no adapter is needed.
type Storage interface {
	// Read copies the extent at loff into buf, returning bytes read.
	Read(buf []byte, loff uint64) (int, error)

	// Insert writes buf as a new extent at loff, shifting existing data.
	Insert(buf []byte, loff uint64) (int, error)

	// Update rewrites the extent at loff (of old length olen) with buf.
	Update(buf []byte, loff, olen uint64) (int, error)

	// Collapse removes olen bytes starting at loff.
	Collapse(loff, length uint64) error

	// SetTag sets the 16-bit metadata tag on the extent at loff.
	SetTag(loff uint64, tag uint16) error

	// IterateExtents calls fn for each extent in [start, end).
	// Iteration stops when fn returns false.
	IterateExtents(start, end uint64, fn func(loff uint64, tag uint16, data []byte) bool)

	// Size returns the total logical size of the store in bytes.
	Size() uint64

	// Sync flushes all pending writes to durable storage.
	Sync()

	// Close releases all resources held by the store.
	Close() error
}

// openStorage opens the default flexfile-backed Storage at path.
// Swap this call in openTable to use a different backend.
func openStorage(path string) (Storage, error) {
	return flexfile.Open(path)
}
