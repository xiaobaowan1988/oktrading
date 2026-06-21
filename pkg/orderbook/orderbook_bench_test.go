package orderbook_test

import (
	"fmt"
	"testing"

	"github.com/xiaobaowan1988/oktrading/pkg/orderbook"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newPopulatedBook(bids, asks int) *orderbook.OrderBook {
	ob := orderbook.New("BTC-USDT")
	for i := 0; i < asks; i++ {
		ob.AddOrder(&types.Order{
			OrderID:   uint64(i + 1),
			Side:      types.Sell,
			Type:      types.Limit,
			Price:     int64(50000 + i + 1),
			Quantity:  100,
			Remaining: 100,
		})
	}
	for i := 0; i < bids; i++ {
		ob.AddOrder(&types.Order{
			OrderID:   uint64(asks + i + 1),
			Side:      types.Buy,
			Type:      types.Limit,
			Price:     int64(49999 - i),
			Quantity:  100,
			Remaining: 100,
		})
	}
	return ob
}

// ── AddOrder ─────────────────────────────────────────────────────────────────

func BenchmarkAddOrder_NewLevel(b *testing.B) {
	// Every insertion creates a brand-new price level.
	b.ReportAllocs()
	ob := orderbook.New("BTC-USDT")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.AddOrder(&types.Order{
			OrderID:   uint64(i + 1),
			Side:      types.Sell,
			Type:      types.Limit,
			Price:     int64(50000 + i),
			Quantity:  1,
			Remaining: 1,
		})
	}
}

func BenchmarkAddOrder_ExistingLevel(b *testing.B) {
	// All orders land on the same price level – only list append, no tree insert.
	b.ReportAllocs()
	ob := orderbook.New("BTC-USDT")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.AddOrder(&types.Order{
			OrderID:   uint64(i + 1),
			Side:      types.Sell,
			Type:      types.Limit,
			Price:     50000,
			Quantity:  1,
			Remaining: 1,
		})
	}
}

// ── CancelOrder ───────────────────────────────────────────────────────────────

func BenchmarkCancelOrder(b *testing.B) {
	b.ReportAllocs()

	const poolSize = 1 << 16
	ob := newPopulatedBook(0, poolSize)

	b.ResetTimer()
	id := uint64(1)
	for i := 0; i < b.N; i++ {
		if id > poolSize {
			id = 1
		}
		// May return false if already cancelled; that's fine for throughput measurement.
		ob.CancelOrder(id)
		id++
	}
}

// ── Match: no fill ────────────────────────────────────────────────────────────

func BenchmarkMatch_NoFill(b *testing.B) {
	// Taker price doesn't cross the spread; just the "check best ask, stop" path.
	b.ReportAllocs()
	ob := newPopulatedBook(0, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ob.Match(&types.Order{
			Side:      types.Buy,
			Type:      types.Limit,
			Price:     1, // far below all asks
			Remaining: 100,
		})
	}
}

// ── Match: single-level fill ──────────────────────────────────────────────────

func BenchmarkMatch_SingleLevel(b *testing.B) {
	// Each iteration: one taker fills exactly one maker, then the maker is re-added.
	b.ReportAllocs()
	ob := newPopulatedBook(0, 1)
	makerID := uint64(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		taker := &types.Order{
			Side:      types.Buy,
			Type:      types.Market,
			Remaining: 1,
		}
		ob.Match(taker)

		// Replenish the consumed maker so the book never runs dry.
		makerID++
		ob.AddOrder(&types.Order{
			OrderID:   makerID,
			Side:      types.Sell,
			Type:      types.Limit,
			Price:     50001,
			Remaining: 1,
		})
	}
}

// ── Match: sweep N levels ─────────────────────────────────────────────────────

func BenchmarkMatch_SweepLevels(b *testing.B) {
	for _, levels := range []int{1, 5, 20, 50} {
		levels := levels
		b.Run(fmt.Sprintf("levels=%d", levels), func(b *testing.B) {
			b.ReportAllocs()

			// pre-compute a fresh book with `levels` ask levels for each iteration
			makeBook := func() *orderbook.OrderBook {
				ob := orderbook.New("BTC-USDT")
				for i := 0; i < levels; i++ {
					ob.AddOrder(&types.Order{
						OrderID:   uint64(i + 1),
						Side:      types.Sell,
						Type:      types.Limit,
						Price:     int64(50000 + i),
						Quantity:  1,
						Remaining: 1,
					})
				}
				return ob
			}

			// One book per outer iteration (setup outside timer).
			books := make([]*orderbook.OrderBook, b.N)
			for i := range books {
				books[i] = makeBook()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ob := books[i]
				ob.Match(&types.Order{
					Side:      types.Buy,
					Type:      types.Market,
					Quantity:  int64(levels),
					Remaining: int64(levels),
				})
			}
		})
	}
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

func BenchmarkSnapshot(b *testing.B) {
	for _, depth := range []int{10, 50, 100} {
		depth := depth
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			ob := newPopulatedBook(depth, depth)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ob.Snapshot(depth)
			}
		})
	}
}
