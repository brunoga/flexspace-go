package flexdb

import "sync/atomic"

// Metrics holds database performance counters.
type Metrics struct {
	PutCount         atomic.Uint64
	GetCount         atomic.Uint64
	DeleteCount      atomic.Uint64
	CacheHitCount    atomic.Uint64
	CacheMissCount   atomic.Uint64
	WALWriteCount    atomic.Uint64
	WALFlushCount    atomic.Uint64
	FlushBatchCount  atomic.Uint64
	GCReclaimedBytes atomic.Uint64
	GCMovedBytes     atomic.Uint64
}

// Snapshot returns a point-in-time copy of the metrics.
type MetricsSnapshot struct {
	PutCount         uint64
	GetCount         uint64
	DeleteCount      uint64
	CacheHitCount    uint64
	CacheMissCount   uint64
	WALWriteCount    uint64
	WALFlushCount    uint64
	FlushBatchCount  uint64
	GCReclaimedBytes uint64
	GCMovedBytes     uint64
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		PutCount:         m.PutCount.Load(),
		GetCount:         m.GetCount.Load(),
		DeleteCount:      m.DeleteCount.Load(),
		CacheHitCount:    m.CacheHitCount.Load(),
		CacheMissCount:   m.CacheMissCount.Load(),
		WALWriteCount:    m.WALWriteCount.Load(),
		WALFlushCount:    m.WALFlushCount.Load(),
		FlushBatchCount:  m.FlushBatchCount.Load(),
		GCReclaimedBytes: m.GCReclaimedBytes.Load(),
		GCMovedBytes:     m.GCMovedBytes.Load(),
	}
}
