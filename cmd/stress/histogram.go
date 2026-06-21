package main

import (
	"fmt"
	"math/bits"
	"strings"
	"sync/atomic"
)

// LatencyHistogram is a concurrent, allocation-free power-of-2 bucket histogram.
//
// Bucket i covers the range [2^i ns, 2^(i+1) ns).
// bits.Len64 maps any int64 value to its bucket in a single instruction.
// 64 buckets cover 1 ns → 2^63 ns (~292 years) with ~50% relative error per bucket.
//
// All methods are safe for concurrent use via atomic bucket counters, so each
// goroutine can record without locks and histograms can be merged at the end.
type LatencyHistogram struct {
	buckets [64]int64 // atomic bucket counts
	count   int64     // atomic total samples
	sum     int64     // atomic sum (ns), used for mean
	max_    int64     // atomic max (ns)
}

// Record stores one latency sample.
func (h *LatencyHistogram) Record(ns int64) {
	if ns <= 0 {
		ns = 1
	}
	b := bits.Len64(uint64(ns)) // b ∈ [1, 64]; bucket index = b-1
	if b > 63 {
		b = 63
	}
	atomic.AddInt64(&h.buckets[b-1], 1)
	atomic.AddInt64(&h.count, 1)
	atomic.AddInt64(&h.sum, ns)
	// Optimistic CAS loop for max (rarely contested).
	for {
		cur := atomic.LoadInt64(&h.max_)
		if ns <= cur {
			break
		}
		if atomic.CompareAndSwapInt64(&h.max_, cur, ns) {
			break
		}
	}
}

// Count returns the total number of recorded samples.
func (h *LatencyHistogram) Count() int64 { return atomic.LoadInt64(&h.count) }

// Max returns the maximum recorded latency in nanoseconds.
func (h *LatencyHistogram) Max() int64 { return atomic.LoadInt64(&h.max_) }

// Mean returns the arithmetic mean latency in nanoseconds.
func (h *LatencyHistogram) Mean() float64 {
	n := atomic.LoadInt64(&h.count)
	if n == 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&h.sum)) / float64(n)
}

// Percentile returns the approximate latency at the given percentile p ∈ [0, 1].
// The reported value is the midpoint of the containing bucket (±50% relative error).
func (h *LatencyHistogram) Percentile(p float64) int64 {
	n := atomic.LoadInt64(&h.count)
	if n == 0 {
		return 0
	}
	target := int64(float64(n) * p)
	if target >= n {
		target = n - 1
	}
	cumulative := int64(0)
	for i := 0; i < 64; i++ {
		cumulative += atomic.LoadInt64(&h.buckets[i])
		if cumulative > target {
			// Midpoint of bucket i: 2^i * 1.5, rounded to nearest ns.
			low := int64(1) << i
			return low + low>>1
		}
	}
	return atomic.LoadInt64(&h.max_)
}

// Merge adds all samples from other into h.
func (h *LatencyHistogram) Merge(other *LatencyHistogram) {
	for i := 0; i < 64; i++ {
		atomic.AddInt64(&h.buckets[i], atomic.LoadInt64(&other.buckets[i]))
	}
	atomic.AddInt64(&h.count, atomic.LoadInt64(&other.count))
	atomic.AddInt64(&h.sum, atomic.LoadInt64(&other.sum))
	otherMax := atomic.LoadInt64(&other.max_)
	for {
		cur := atomic.LoadInt64(&h.max_)
		if otherMax <= cur {
			break
		}
		if atomic.CompareAndSwapInt64(&h.max_, cur, otherMax) {
			break
		}
	}
}

// ─── Human-readable report ────────────────────────────────────────────────────

func fmtNS(ns int64) string {
	switch {
	case ns < 1_000:
		return fmt.Sprintf("%d ns", ns)
	case ns < 1_000_000:
		return fmt.Sprintf("%.2f µs", float64(ns)/1e3)
	case ns < 1_000_000_000:
		return fmt.Sprintf("%.2f ms", float64(ns)/1e6)
	default:
		return fmt.Sprintf("%.3f s", float64(ns)/1e9)
	}
}

func (h *LatencyHistogram) Report(title string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s  (n=%d)\n", title, h.Count())
	if h.Count() == 0 {
		sb.WriteString("  <no samples>\n")
		return sb.String()
	}
	rows := []struct {
		label string
		val   int64
	}{
		{"  mean  ", int64(h.Mean())},
		{"  p50   ", h.Percentile(0.50)},
		{"  p90   ", h.Percentile(0.90)},
		{"  p99   ", h.Percentile(0.99)},
		{"  p99.9 ", h.Percentile(0.999)},
		{"  p99.99", h.Percentile(0.9999)},
		{"  max   ", h.Max()},
	}
	for _, r := range rows {
		fmt.Fprintf(&sb, "%s : %s\n", r.label, fmtNS(r.val))
	}
	return sb.String()
}
