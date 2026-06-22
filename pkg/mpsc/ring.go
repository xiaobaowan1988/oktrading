// Package mpsc implements a lock-free multi-producer single-consumer ring
// buffer for *types.Command values.
//
// # Why not a Go channel?
//
// Go's buffered channel uses an internal mutex.  Every TrySubmit call —
// even a non-blocking one that immediately returns false — acquires that
// mutex, serialising all producers completely.  Under N concurrent
// producers this creates a single-point contention that limits throughput
// to one write per mutex round-trip (~50–200 ns).
//
// # How this ring works
//
// Each slot carries a lap counter.  A "lap" is the number of times the
// ring cursor has passed through that slot index:
//
//	lap = sequence >> log₂(capacity)
//
// Lifecycle of slot[i]:
//
//	Initial   : lap = MaxUint64  (never published sentinel)
//	Published : lap = seq >> log₂(cap)  written by the producer that claimed seq
//	Consumed  : consumer advances tail; lap stays until overwritten next lap
//
// # Producer protocol
//
//  1. Load cursor, check ring is not full (cursor − tail < cap).
//  2. CAS(cursor, cur, cur+1).  If it fails another producer won; retry.
//  3. Write cmd to slot[cur & mask].  (Different producers write to
//     different slots — these writes happen in parallel, no conflict.)
//  4. Atomic-store lap = cur >> log₂.  This is the "publish" signal.
//
// # Consumer protocol
//
//  1. Load tail.
//  2. If slot[tail & mask].lap == tail >> log₂  → slot is ready.
//  3. Read cmd, clear slot, advance tail.
//
// The only serialisation point is the single CAS on the cursor.  Once a
// producer wins the CAS it writes to its own exclusive slot concurrently
// with all other producers writing to theirs.
package mpsc

import (
	"math/bits"
	"sync/atomic"

	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

const _never = ^uint64(0) // MaxUint64: slot not yet published

// Ring is the lock-free MPSC ring buffer.
type Ring struct {
	slots []cmdSlot // Go heap; len is always a power of two
	log2  uint64    // log₂(len(slots))
	mask  uint64    // len(slots) − 1
	_     [24]byte  // pad header to 64 bytes  (24+8+8+24 = 64)
	prod  struct {
		cursor atomic.Uint64
		_      [56]byte // pad producer line to 64 bytes
	}
	cons struct {
		tail atomic.Uint64 // read by producers for back-pressure check
		_    [56]byte      // pad consumer line to 64 bytes
	}
}

// cmdSlot occupies exactly one 64-byte cache line.
type cmdSlot struct {
	lap atomic.Uint64  // publish signal; _never until claimed+written
	cmd *types.Command // pointer written before lap is published
	_   [48]byte       // pad to 64 bytes  (8 + 8 + 48 = 64)
}

// New allocates a Ring with at least capacity slots (rounded up to the
// next power of two, minimum 2).
func New(capacity int) *Ring {
	size := nextPow2(capacity)
	log2 := uint64(bits.Len(uint(size)) - 1)
	slots := make([]cmdSlot, size)
	for i := range slots {
		slots[i].lap.Store(_never) // mark all slots unpublished
	}
	return &Ring{
		slots: slots,
		log2:  log2,
		mask:  uint64(size - 1),
	}
}

// TrySubmit tries to enqueue cmd without blocking.
// Returns false when the ring is full (back-pressure signal).
// Safe for any number of concurrent callers.
func (r *Ring) TrySubmit(cmd *types.Command) bool {
	for {
		cur := r.prod.cursor.Load()

		// Back-pressure: ring full?
		if cur-r.cons.tail.Load() >= uint64(len(r.slots)) {
			return false
		}

		// Race to claim slot cur.  Exactly one producer wins per CAS.
		if !r.prod.cursor.CompareAndSwap(cur, cur+1) {
			// Another producer claimed cur first; reload and retry.
			// No Gosched: the retry loop is O(producers) and stays
			// in userspace — cheaper than yielding to the scheduler.
			continue
		}

		// We exclusively own slot[cur & mask].  Write then publish.
		s := &r.slots[cur&r.mask]
		s.cmd = cmd
		// Atomic store acts as a release fence: cmd write is visible
		// to the consumer before the lap number is.
		s.lap.Store(cur >> r.log2)
		return true
	}
}

// Submit enqueues cmd, spinning until space is available.
// Prefer TrySubmit on hot paths to handle back-pressure explicitly.
func (r *Ring) Submit(cmd *types.Command) {
	for !r.TrySubmit(cmd) {
	}
}

// TryPop removes and returns the next command.
// Returns nil when the ring is empty.
// Must be called from exactly one goroutine (the engine).
func (r *Ring) TryPop() *types.Command {
	tail := r.cons.tail.Load()
	s := &r.slots[tail&r.mask]

	// Wait for the producer that claimed `tail` to publish.
	if s.lap.Load() != tail>>r.log2 {
		return nil // not yet published
	}

	cmd := s.cmd
	s.cmd = nil // clear GC reference
	r.cons.tail.Add(1)
	return cmd
}

// Len returns the number of unconsumed slots (approximate under concurrency).
func (r *Ring) Len() int {
	prod := r.prod.cursor.Load()
	cons := r.cons.tail.Load()
	if prod <= cons {
		return 0
	}
	n := prod - cons
	if n > uint64(len(r.slots)) {
		return len(r.slots)
	}
	return int(n)
}

func nextPow2(n int) int {
	if n <= 2 {
		return 2
	}
	return 1 << bits.Len(uint(n-1))
}
