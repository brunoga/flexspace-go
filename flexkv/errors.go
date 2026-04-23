package flexkv

import "errors"

var (
	// ErrTableNotFound is returned when a table does not exist.
	ErrTableNotFound = errors.New("flexkv: table not found")

	// ErrIndexNotFound is returned when an index does not exist on a table.
	ErrIndexNotFound = errors.New("flexkv: index not found")

	// ErrInvalidIndexEntry is returned when an index iterator points to a malformed entry.
	ErrInvalidIndexEntry = errors.New("flexkv: invalid index entry")
)
