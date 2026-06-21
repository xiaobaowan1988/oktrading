package disruptor_test

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/xiaobaowan1988/oktrading/pkg/disruptor"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// sentinel is the dummy value pushed/popped throughout.
var sentinel = &types.Command{}

// ── Single-goroutine round-trip ───────────────────────────────────────────────

func BenchmarkRingBuffer_SameGoroutine(b *testing.B) {
	// Both push and pop happen in the same goroutine: measures cache hit cost.
	rb := disruptor.New[types.Command](1024)
	b.SetBytes(int64(unsafe.Sizeof(types.Command{})))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.TryPush(sentinel)
		rb.TryPop()
	}
}

// ── SPSC cross-goroutine throughput ──────────────────────────────────────────

func BenchmarkRingBuffer_SPSC(b *testing.B) {
	for _, cap_ := range []int{64, 1024, 16384} {
		cap_ := cap_
		b.Run(fmt.Sprintf("cap=%d", cap_), func(b *testing.B) {
			rb := disruptor.New[types.Command](cap_)
			b.SetBytes(int64(unsafe.Sizeof(types.Command{})))
			b.ResetTimer()

			var wg sync.WaitGroup
			wg.Add(2)

			// Producer
			go func() {
				defer wg.Done()
				for i := 0; i < b.N; i++ {
					rb.Push(sentinel)
				}
			}()

			// Consumer
			go func() {
				defer wg.Done()
				for i := 0; i < b.N; i++ {
					rb.Pop()
				}
			}()

			wg.Wait()
		})
	}
}

// ── Back-pressure (full buffer) ───────────────────────────────────────────────

func BenchmarkRingBuffer_FullReject(b *testing.B) {
	// Fill the buffer completely, then measure the cost of TryPush returning false.
	rb := disruptor.New[types.Command](64)
	for i := 0; i < 64; i++ {
		rb.TryPush(sentinel)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.TryPush(sentinel) // always false
	}
}

// ── Empty drain ───────────────────────────────────────────────────────────────

func BenchmarkRingBuffer_EmptyPop(b *testing.B) {
	// Measure TryPop returning false (busy-wait cost).
	rb := disruptor.New[types.Command](64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.TryPop()
	}
}
