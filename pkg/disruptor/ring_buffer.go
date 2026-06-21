// Package disruptor provides a lock-free single-producer / single-consumer
// (SPSC) ring buffer modelled after the LMAX Disruptor pattern.
//
// # Memory-ordering proof
//
// Let P be the producer goroutine and C the consumer goroutine.
//
//	P: slots[head & mask].Store(item)   [W_data]
//	P: head.Add(1)                      [W_head]
//	C: head.Load() → sees new value     [R_head]   synchronizes-with W_head
//	C: slots[tail & mask].Load()        [R_data]
//
// W_data hb W_head  (program order, same goroutine P)
// W_head  sw R_head  (atomic store → load of the same location, C sees new value)
// R_head  hb R_data  (program order, same goroutine C)
// ∴ W_data hb R_data  (transitivity) — the stored item is visible to the consumer.
//
// Because this is SPSC, no producer-producer or consumer-consumer races exist.
// Capacity must be a power of two for the bitmask modulo trick.
package disruptor

import (
	"runtime"
	"sync/atomic"
)

const cacheLinePad = 56 // 64-byte cache line minus 8 bytes for the counter itself

// producerState occupies one cache line to prevent false sharing with the
// consumer's tail counter.
type producerState struct {
	head atomic.Uint64
	_    [cacheLinePad]byte
}

// consumerState occupies one cache line to prevent false sharing with the
// producer's head counter.
type consumerState struct {
	tail atomic.Uint64
	_    [cacheLinePad]byte
}

// RingBuffer[T] is a generic SPSC ring buffer.  T must be a pointer type;
// the internal slots store *T via atomic.Pointer[T].
type RingBuffer[T any] struct {
	slots    []atomic.Pointer[T]
	mask     uint64
	producer producerState
	consumer consumerState
}

// New allocates a ring buffer of size ≥ capacity, rounded up to the next
// power of two.
func New[T any](capacity int) *RingBuffer[T] {
	size := nextPow2(capacity)
	return &RingBuffer[T]{
		slots: make([]atomic.Pointer[T], size),
		mask:  uint64(size - 1),
	}
}

// TryPush enqueues item without blocking.
// Returns false if the buffer is at capacity (back-pressure signal to caller).
func (rb *RingBuffer[T]) TryPush(item *T) bool {
	head := rb.producer.head.Load()
	tail := rb.consumer.tail.Load()
	if head-tail >= uint64(len(rb.slots)) {
		return false
	}
	rb.slots[head&rb.mask].Store(item)
	rb.producer.head.Add(1)
	return true
}

// Push enqueues item, spinning with a Gosched yield on each failed attempt.
// The yield prevents the engine goroutine from monopolising the CPU while the
// slow consumer drains the buffer.
func (rb *RingBuffer[T]) Push(item *T) {
	for !rb.TryPush(item) {
		runtime.Gosched()
	}
}

// TryPop dequeues the next item without blocking.
// Returns (nil, false) if the buffer is empty.
func (rb *RingBuffer[T]) TryPop() (*T, bool) {
	tail := rb.consumer.tail.Load()
	head := rb.producer.head.Load()
	if head == tail {
		return nil, false
	}
	item := rb.slots[tail&rb.mask].Load()
	rb.consumer.tail.Add(1)
	return item, true
}

// Pop dequeues the next item, spinning until one is available.
func (rb *RingBuffer[T]) Pop() *T {
	for {
		if item, ok := rb.TryPop(); ok {
			return item
		}
		runtime.Gosched()
	}
}

// Len returns an approximate count of unconsumed items.
// Under concurrent access the value may be transiently stale.
func (rb *RingBuffer[T]) Len() uint64 {
	head := rb.producer.head.Load()
	tail := rb.consumer.tail.Load()
	if head < tail {
		return 0 // wrap-around guard for snapshot races
	}
	return head - tail
}

// Cap returns the ring buffer's fixed capacity.
func (rb *RingBuffer[T]) Cap() int { return len(rb.slots) }

// nextPow2 returns the smallest power of two ≥ n (minimum 1).
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
