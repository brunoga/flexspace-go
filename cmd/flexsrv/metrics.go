package main

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// latencyBounds are histogram bucket upper bounds in milliseconds.
// Covers sub-millisecond storage ops through slow bulk operations.
var latencyBounds = []float64{0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000}

const numBuckets = 12 // len(latencyBounds) + 1; last bucket catches everything >1000 ms

// routeMetrics holds atomic counters for one registered route pattern.
type routeMetrics struct {
	requests atomic.Int64
	errors   atomic.Int64 // responses with HTTP status >= 400
	active   atomic.Int64 // in-flight requests right now
	sumNs    atomic.Int64 // total latency in nanoseconds (for mean)
	minNs    atomic.Int64 // minimum observed latency; initialised to MaxInt64
	maxNs    atomic.Int64
	// buckets[i] counts requests whose latency fell in (latencyBounds[i-1], latencyBounds[i]].
	// buckets[numBuckets-1] is the overflow (> 1000 ms).
	buckets [numBuckets]atomic.Int64
}

// Metrics tracks per-route request counters and latency histograms.
// All methods are safe for concurrent use.
type Metrics struct {
	startTime time.Time
	mu        sync.RWMutex
	routes    map[string]*routeMetrics
}

func newMetrics() *Metrics {
	return &Metrics{
		startTime: time.Now(),
		routes:    make(map[string]*routeMetrics),
	}
}

func (m *Metrics) getOrCreate(route string) *routeMetrics {
	m.mu.RLock()
	rm := m.routes[route]
	m.mu.RUnlock()
	if rm != nil {
		return rm
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if rm = m.routes[route]; rm != nil {
		return rm
	}
	rm = &routeMetrics{}
	rm.minNs.Store(math.MaxInt64)
	m.routes[route] = rm
	return rm
}

// record updates counters for a completed request.
func (m *Metrics) record(route string, status int, dur time.Duration) {
	rm := m.getOrCreate(route)
	rm.requests.Add(1)
	if status >= 400 {
		rm.errors.Add(1)
	}

	ns := dur.Nanoseconds()
	rm.sumNs.Add(ns)

	// CAS loops to update min/max without a mutex.
	for cur := rm.minNs.Load(); ns < cur; cur = rm.minNs.Load() {
		if rm.minNs.CompareAndSwap(cur, ns) {
			break
		}
	}
	for cur := rm.maxNs.Load(); ns > cur; cur = rm.maxNs.Load() {
		if rm.maxNs.CompareAndSwap(cur, ns) {
			break
		}
	}

	// Place in the appropriate histogram bucket.
	ms := float64(ns) / 1e6
	bi := numBuckets - 1
	for i, bound := range latencyBounds {
		if ms <= bound {
			bi = i
			break
		}
	}
	rm.buckets[bi].Add(1)
}

// routeSnapshot returns a JSON-serialisable map for one route.
func (m *Metrics) routeSnapshot(route string) map[string]any {
	rm := m.getOrCreate(route)
	reqs := rm.requests.Load()

	snap := map[string]any{
		"requests": reqs,
		"errors":   rm.errors.Load(),
		"active":   rm.active.Load(),
	}

	if reqs > 0 {
		// Collect raw (non-cumulative) bucket counts.
		raw := make([]int64, numBuckets)
		for i := range raw {
			raw[i] = rm.buckets[i].Load()
		}

		// Build cumulative histogram for the output.
		hist := make(map[string]int64, numBuckets)
		var cumul int64
		for i, bound := range latencyBounds {
			cumul += raw[i]
			hist[fmt.Sprintf("le_%.4g", bound)] = cumul
		}
		cumul += raw[numBuckets-1]
		hist["+Inf"] = cumul

		minNs := rm.minNs.Load()
		maxNs := rm.maxNs.Load()
		snap["latency_ms"] = map[string]any{
			"mean":      roundMS(float64(rm.sumNs.Load()) / float64(reqs)),
			"min":       roundMS(float64(minNs)),
			"max":       roundMS(float64(maxNs)),
			"p50":       roundMS(percentileMS(raw, reqs, 0.50)),
			"p90":       roundMS(percentileMS(raw, reqs, 0.90)),
			"p95":       roundMS(percentileMS(raw, reqs, 0.95)),
			"p99":       roundMS(percentileMS(raw, reqs, 0.99)),
			"histogram": hist,
		}
	}

	return snap
}

// Snapshot returns a full JSON-serialisable snapshot of all collected metrics.
func (m *Metrics) Snapshot() map[string]any {
	m.mu.RLock()
	routes := make([]string, 0, len(m.routes))
	for k := range m.routes {
		routes = append(routes, k)
	}
	m.mu.RUnlock()

	routeSnaps := make(map[string]any, len(routes))
	for _, r := range routes {
		routeSnaps[r] = m.routeSnapshot(r)
	}

	return map[string]any{
		"uptime_seconds": math.Round(time.Since(m.startTime).Seconds()),
		"routes":         routeSnaps,
	}
}

// percentileMS estimates the p-th percentile latency in milliseconds using
// linear interpolation within the bucket that contains the target rank.
func percentileMS(counts []int64, total int64, p float64) float64 {
	target := int64(math.Ceil(p * float64(total)))
	var cumul int64
	for i, c := range counts {
		cumul += c
		if cumul >= target {
			var lo, hi float64
			if i > 0 {
				lo = latencyBounds[i-1]
			}
			if i < len(latencyBounds) {
				hi = latencyBounds[i]
			} else {
				// Overflow bucket: extrapolate to 2× the last bound.
				hi = latencyBounds[len(latencyBounds)-1] * 2
			}
			prev := cumul - c
			frac := float64(target-prev) / float64(c)
			return lo + frac*(hi-lo)
		}
	}
	return 0
}

// roundMS rounds to 2 decimal places and converts nanoseconds to milliseconds
// when called with a nanosecond value.
func roundMS(ns float64) float64 {
	ms := ns / 1e6
	return math.Round(ms*100) / 100
}
