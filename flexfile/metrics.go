package flexfile

import "sync/atomic"

// Metrics defines the interface for collecting low-level storage metrics.
type Metrics struct {
	WALWriteCount    *atomic.Uint64
	GCReclaimedBytes *atomic.Uint64
	GCMovedBytes     *atomic.Uint64
}
