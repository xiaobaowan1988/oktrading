package mpsc_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/xiaobaowan1988/oktrading/pkg/mpsc"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

func makeCmd(id uint64) *types.Command {
	return &types.Command{
		Type:  types.CmdNewOrder,
		Order: &types.Order{OrderID: id},
	}
}

// TestSingleProducer verifies FIFO order with one producer.
func TestSingleProducer(t *testing.T) {
	r := mpsc.New(128) // capacity must be >= n
	const n = 100
	for i := uint64(1); i <= n; i++ {
		if !r.TrySubmit(makeCmd(i)) {
			t.Fatalf("TrySubmit failed at i=%d", i)
		}
	}
	for i := uint64(1); i <= n; i++ {
		cmd := r.TryPop()
		if cmd == nil {
			t.Fatalf("TryPop returned nil at i=%d", i)
		}
		if cmd.Order.OrderID != i {
			t.Fatalf("want OrderID=%d, got %d", i, cmd.Order.OrderID)
		}
	}
	if r.TryPop() != nil {
		t.Fatal("ring should be empty")
	}
}

// TestBackPressure verifies TrySubmit returns false when full.
func TestBackPressure(t *testing.T) {
	const cap = 8
	r := mpsc.New(cap)
	for i := 0; i < cap; i++ {
		if !r.TrySubmit(makeCmd(uint64(i + 1))) {
			t.Fatalf("should have space at slot %d", i)
		}
	}
	if r.TrySubmit(makeCmd(99)) {
		t.Fatal("ring is full, TrySubmit should return false")
	}
	// Drain one slot, then submit should succeed again.
	r.TryPop()
	if !r.TrySubmit(makeCmd(100)) {
		t.Fatal("one slot freed, TrySubmit should succeed")
	}
}

// TestMultiProducer spawns N producers and verifies every command is
// received exactly once.  Does not check ordering across producers
// (arrival order is non-deterministic by design).
func TestMultiProducer(t *testing.T) {
	const (
		producers  = 8
		perProd    = 10_000
		ringCap    = 1 << 17 // large enough to avoid back-pressure
	)

	r := mpsc.New(ringCap)
	var wg sync.WaitGroup

	// Start producers.
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < perProd; i++ {
				id := base + i
				for !r.TrySubmit(makeCmd(id)) {
					// back-pressure: spin
				}
			}
		}(uint64(p * perProd))
	}

	// Consumer: drain until all commands received.
	total := producers * perProd
	seen := make(map[uint64]int, total)
	received := 0

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	for received < total {
		cmd := r.TryPop()
		if cmd == nil {
			continue
		}
		seen[cmd.Order.OrderID]++
		received++
	}

	// Verify each ID received exactly once.
	for p := 0; p < producers; p++ {
		for i := 0; i < perProd; i++ {
			id := uint64(p*perProd + i)
			if seen[id] != 1 {
				t.Errorf("OrderID=%d seen %d times (want 1)", id, seen[id])
			}
		}
	}
}

// TestRaceDetector runs the multi-producer test under -race.
func TestRaceDetector(t *testing.T) {
	const (
		producers = 4
		perProd   = 1_000
	)
	r := mpsc.New(1 << 14)
	var submitted atomic.Int64
	var wg sync.WaitGroup

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := uint64(0); i < perProd; i++ {
				for !r.TrySubmit(makeCmd(base + i)) {
				}
				submitted.Add(1)
			}
		}(uint64(p * perProd))
	}

	go wg.Wait()

	received := 0
	for received < producers*perProd {
		if cmd := r.TryPop(); cmd != nil {
			_ = cmd
			received++
		}
	}
}

// BenchmarkTrySubmit_1P measures single-producer throughput.
func BenchmarkTrySubmit_1P(b *testing.B) {
	r := mpsc.New(1 << 20)
	cmd := makeCmd(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for !r.TrySubmit(cmd) {
			r.TryPop()
		}
		r.TryPop()
	}
}

// BenchmarkTrySubmit_4P measures 4-producer throughput (consumer inline).
func BenchmarkTrySubmit_4P(b *testing.B) {
	r := mpsc.New(1 << 20)
	cmd := makeCmd(1)
	var wg sync.WaitGroup
	b.ResetTimer()

	perP := b.N / 4
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perP; i++ {
				for !r.TrySubmit(cmd) {
				}
			}
		}()
	}

	total := perP * 4
	for received := 0; received < total; {
		if r.TryPop() != nil {
			received++
		}
	}
	wg.Wait()
}
