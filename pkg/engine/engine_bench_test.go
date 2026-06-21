package engine_test

import (
	"testing"

	"github.com/xiaobaowan1988/oktrading/pkg/engine"
	"github.com/xiaobaowan1988/oktrading/pkg/offheap"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// drainN consumes exactly n events from the engine's outbound buffer.
// Called after the benchmark timer stops to flush the pipeline.
func drainN(e *engine.Engine, n int) {
	var dst offheap.RawEvent
	for received := 0; received < n; {
		if e.TryPollEvent(&dst) {
			received++
		}
	}
}

// prefillBook submits n resting sell orders and drains their accepted events.
func prefillBook(e *engine.Engine, n int) {
	for i := 0; i < n; i++ {
		e.Submit(&types.Command{
			Type: types.CmdNewOrder,
			Order: &types.Order{
				OrderID:   uint64(i + 1),
				Symbol:    "BTC-USDT",
				Side:      types.Sell,
				Type:      types.Limit,
				Price:     int64(50_000 + i + 1),
				Quantity:  1,
				Remaining: 1,
			},
		})
	}
	drainN(e, n) // drain all accepted events before timing starts
}

// startEngine creates and starts a fresh engine, registering cleanup.
func startEngine(b *testing.B) *engine.Engine {
	b.Helper()
	e := engine.New("BTC-USDT", 1<<16, 1<<16)
	e.Start()
	b.Cleanup(func() { e.Stop() })
	return e
}

// ── Resting-order path ────────────────────────────────────────────────────────

func BenchmarkEngine_RestingLimit(b *testing.B) {
	// Submit orders that never match (alternating bid/ask far from mid).
	// One EvtOrderAccepted per order → b.N events total.
	b.ReportAllocs()
	e := startEngine(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		side := types.Buy
		price := int64(40_000)
		if i%2 == 1 {
			side = types.Sell
			price = 60_000
		}
		e.Submit(&types.Command{
			Type: types.CmdNewOrder,
			Order: &types.Order{
				OrderID:   uint64(i + 1),
				Symbol:    "BTC-USDT",
				Side:      side,
				Type:      types.Limit,
				Price:     price,
				Quantity:  1,
				Remaining: 1,
			},
		})
	}
	b.StopTimer()
	drainN(e, b.N)
}

// ── Immediate-fill path ───────────────────────────────────────────────────────

func BenchmarkEngine_ImmediateFill(b *testing.B) {
	// Pre-fill with 64k resting sells, then benchmark market buys.
	// Each market buy → EvtTrade + EvtOrderFilled = 2 events.
	b.ReportAllocs()
	e := startEngine(b)

	const bookDepth = 1 << 16
	prefillBook(e, bookDepth)

	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if idx >= bookDepth {
			b.StopTimer()
			prefillBook(e, bookDepth) // replenish without timing
			idx = 0
			b.StartTimer()
		}
		e.Submit(&types.Command{
			Type: types.CmdNewOrder,
			Order: &types.Order{
				OrderID:   uint64(1_000_000 + i),
				Symbol:    "BTC-USDT",
				Side:      types.Buy,
				Type:      types.Market,
				Quantity:  1,
				Remaining: 1,
			},
		})
		idx++
	}
	b.StopTimer()
	drainN(e, b.N*2)
}

// ── Cancel path ───────────────────────────────────────────────────────────────

func BenchmarkEngine_Cancel(b *testing.B) {
	// Submit N resting orders, then benchmark N cancel commands.
	b.ReportAllocs()
	e := startEngine(b)
	prefillBook(e, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Submit(&types.Command{
			Type:     types.CmdCancelOrder,
			CancelID: uint64(i + 1),
		})
	}
	b.StopTimer()
	drainN(e, b.N)
}

// ── Mixed realistic workload ──────────────────────────────────────────────────

func BenchmarkEngine_Mixed(b *testing.B) {
	// 70% resting limit, 20% market taker, 10% cancel
	b.ReportAllocs()
	e := startEngine(b)
	prefillBook(e, 1<<16)

	cancelIDs := make([]uint64, 0, 1000)
	nextID := uint64(2_000_000)
	totalEvents := 0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		roll := i % 10
		switch {
		case roll < 7: // 70% resting limit
			nextID++
			side := types.Sell
			price := int64(60_000 + int(nextID)%1000)
			e.Submit(&types.Command{
				Type: types.CmdNewOrder,
				Order: &types.Order{
					OrderID:   nextID,
					Symbol:    "BTC-USDT",
					Side:      side,
					Type:      types.Limit,
					Price:     price,
					Quantity:  1,
					Remaining: 1,
				},
			})
			cancelIDs = append(cancelIDs, nextID)
			totalEvents++

		case roll < 9: // 20% market taker
			e.Submit(&types.Command{
				Type: types.CmdNewOrder,
				Order: &types.Order{
					OrderID:   nextID + 5_000_000,
					Symbol:    "BTC-USDT",
					Side:      types.Buy,
					Type:      types.Market,
					Quantity:  1,
					Remaining: 1,
				},
			})
			totalEvents += 2 // trade + fill

		default: // 10% cancel
			if len(cancelIDs) > 0 {
				cid := cancelIDs[0]
				cancelIDs = cancelIDs[1:]
				e.Submit(&types.Command{
					Type:     types.CmdCancelOrder,
					CancelID: cid,
				})
				totalEvents++ // cancel confirmation
			}
		}
	}
	b.StopTimer()
	drainN(e, totalEvents)
}
