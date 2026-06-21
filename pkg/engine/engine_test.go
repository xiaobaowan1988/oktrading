package engine_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/xiaobaowan1988/oktrading/pkg/engine"
	"github.com/xiaobaowan1988/oktrading/pkg/offheap"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// pollEvents drains all events from the engine up to a short deadline.
func pollEvents(e *engine.Engine, want int, timeout time.Duration) []offheap.RawEvent {
	var evts []offheap.RawEvent
	deadline := time.Now().Add(timeout)
	var dst offheap.RawEvent
	for time.Now().Before(deadline) {
		if e.TryPollEvent(&dst) {
			evts = append(evts, dst)
			if len(evts) >= want {
				break
			}
		} else {
			time.Sleep(100 * time.Microsecond)
		}
	}
	return evts
}

func newEngine() *engine.Engine {
	e := engine.New("BTC-USDT", 256, 256)
	e.Start()
	return e
}

func submit(e *engine.Engine, cmd *types.Command) {
	e.Submit(cmd)
}

func limitOrder(id uint64, side types.Side, price, qty int64) *types.Order {
	return &types.Order{
		OrderID:   id,
		Symbol:    "BTC-USDT",
		Side:      side,
		Type:      types.Limit,
		Price:     price,
		Quantity:  qty,
		Remaining: qty,
		Timestamp: time.Now().UnixNano(),
	}
}

func marketOrder(id uint64, side types.Side, qty int64) *types.Order {
	return &types.Order{
		OrderID:   id,
		Symbol:    "BTC-USDT",
		Side:      side,
		Type:      types.Market,
		Quantity:  qty,
		Remaining: qty,
		Timestamp: time.Now().UnixNano(),
	}
}

// ── Limit order rests then fills ──────────────────────────────────────────────

func TestLimitOrderAcceptedThenFilled(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	// Place a sell limit; expect EvtOrderAccepted.
	submit(e, &types.Command{
		Type:  types.CmdNewOrder,
		Order: limitOrder(1, types.Sell, 50000, 1),
	})
	evts := pollEvents(e, 1, 2*time.Second)
	if len(evts) < 1 {
		t.Fatal("timed out waiting for accepted event")
	}
	if types.EventType(evts[0].EvtType) != types.EvtOrderAccepted {
		t.Fatalf("want EvtOrderAccepted, got %v", evts[0].EvtType)
	}

	// Cross with a matching buy; expect EvtTrade + EvtOrderFilled for the buy.
	submit(e, &types.Command{
		Type:  types.CmdNewOrder,
		Order: limitOrder(2, types.Buy, 50000, 1),
	})
	evts = pollEvents(e, 2, 2*time.Second)
	if len(evts) < 2 {
		t.Fatalf("want ≥2 events after fill, got %d", len(evts))
	}

	typesMap := make(map[types.EventType]bool)
	for _, ev := range evts {
		typesMap[types.EventType(ev.EvtType)] = true
	}
	if !typesMap[types.EvtTrade] {
		t.Error("expected EvtTrade")
	}
	if !typesMap[types.EvtOrderFilled] {
		t.Error("expected EvtOrderFilled")
	}
}

// ── Market order fills against resting book ───────────────────────────────────

func TestMarketOrderFills(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	// Rest two sell levels.
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(1, types.Sell, 50000, 2)})
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(2, types.Sell, 50100, 2)})
	pollEvents(e, 2, 2*time.Second) // drain accepted events

	// Market buy for 4 units should sweep both levels.
	submit(e, &types.Command{
		Type:  types.CmdNewOrder,
		Order: marketOrder(3, types.Buy, 4),
	})
	evts := pollEvents(e, 3, 2*time.Second) // 2 trades + 1 filled

	var trades int
	for _, ev := range evts {
		if types.EventType(ev.EvtType) == types.EvtTrade {
			trades++
		}
	}
	if trades != 2 {
		t.Errorf("want 2 trades, got %d (total events: %d)", trades, len(evts))
	}
}

// ── IOC cancels residual ──────────────────────────────────────────────────────

func TestIOCCancelsResidue(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(1, types.Sell, 50000, 1)})
	pollEvents(e, 1, 2*time.Second)

	ioc := limitOrder(2, types.Buy, 50000, 5)
	ioc.Type = types.IOC
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: ioc})

	evts := pollEvents(e, 2, 2*time.Second) // trade + cancel

	var sawTrade, sawCancel bool
	for _, ev := range evts {
		if types.EventType(ev.EvtType) == types.EvtTrade {
			sawTrade = true
		}
		if types.EventType(ev.EvtType) == types.EvtOrderCancelled {
			sawCancel = true
		}
	}
	if !sawTrade {
		t.Error("expected EvtTrade for matched portion")
	}
	if !sawCancel {
		t.Error("expected EvtOrderCancelled for residual IOC")
	}
}

// ── FOK rejects on insufficient liquidity ─────────────────────────────────────

func TestFOKRejectInsufficientLiquidity(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(1, types.Sell, 50000, 1)})
	pollEvents(e, 1, 2*time.Second)

	fok := limitOrder(2, types.Buy, 50000, 100) // book only has 1 unit
	fok.Type = types.FOK
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: fok})

	evts := pollEvents(e, 1, 2*time.Second)
	if len(evts) == 0 {
		t.Fatal("expected a rejection event")
	}
	if types.EventType(evts[0].EvtType) != types.EvtOrderRejected {
		t.Errorf("want EvtOrderRejected, got %v", evts[0].EvtType)
	}
	// The resting sell must not have been consumed.
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(3, types.Buy, 50000, 1)})
	evts2 := pollEvents(e, 2, 2*time.Second)
	var sawTrade bool
	for _, ev := range evts2 {
		if types.EventType(ev.EvtType) == types.EvtTrade {
			sawTrade = true
		}
	}
	if !sawTrade {
		t.Error("resting order should still be available after FOK rejection")
	}
}

// ── PostOnly rejects when it would cross ─────────────────────────────────────

func TestPostOnlyRejectedOnCross(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(1, types.Sell, 50000, 1)})
	pollEvents(e, 1, 2*time.Second)

	po := limitOrder(2, types.Buy, 50000, 1) // would cross
	po.Type = types.PostOnly
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: po})

	evts := pollEvents(e, 1, 2*time.Second)
	if len(evts) == 0 {
		t.Fatal("expected rejection event")
	}
	if types.EventType(evts[0].EvtType) != types.EvtOrderRejected {
		t.Errorf("want EvtOrderRejected, got %v", evts[0].EvtType)
	}
}

// ── Cancel ────────────────────────────────────────────────────────────────────

func TestCancelOrder(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(1, types.Buy, 49000, 5)})
	pollEvents(e, 1, 2*time.Second)

	submit(e, &types.Command{
		Type:     types.CmdCancelOrder,
		CancelID: 1,
	})
	evts := pollEvents(e, 1, 2*time.Second)
	if len(evts) == 0 {
		t.Fatal("expected cancel confirmation")
	}
	if types.EventType(evts[0].EvtType) != types.EvtOrderCancelled {
		t.Errorf("want EvtOrderCancelled, got %v", evts[0].EvtType)
	}
}

// ── Sequence numbers are monotone ─────────────────────────────────────────────

func TestSequenceMonotone(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	const n = 10
	for i := 0; i < n; i++ {
		submit(e, &types.Command{
			Type:  types.CmdNewOrder,
			Order: limitOrder(uint64(i+1), types.Buy, int64(49000-i), 1),
		})
	}
	evts := pollEvents(e, n, 3*time.Second)
	if len(evts) < n {
		t.Fatalf("only received %d/%d events", len(evts), n)
	}
	for i := 1; i < len(evts); i++ {
		if evts[i].SeqNo <= evts[i-1].SeqNo {
			t.Errorf("sequence not monotone at index %d: %d <= %d",
				i, evts[i].SeqNo, evts[i-1].SeqNo)
		}
	}
}

// ── Rejection reason is preserved ────────────────────────────────────────────

func TestRejectionReasonPreserved(t *testing.T) {
	e := newEngine()
	defer e.Stop()

	// Place a sell so that a PostOnly buy at the same price would cross.
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: limitOrder(1, types.Sell, 50000, 1)})
	pollEvents(e, 1, 2*time.Second)

	po := limitOrder(2, types.Buy, 50000, 1)
	po.Type = types.PostOnly
	submit(e, &types.Command{Type: types.CmdNewOrder, Order: po})

	evts := pollEvents(e, 1, 2*time.Second)
	if len(evts) == 0 {
		t.Fatal("expected rejection event")
	}
	ev := evts[0]
	if types.EventType(ev.EvtType) != types.EvtOrderRejected {
		t.Fatalf("want EvtOrderRejected, got %v", ev.EvtType)
	}
	reason := string(bytes.TrimRight(ev.Reason[:], "\x00"))
	if reason == "" {
		t.Error("expected non-empty rejection reason")
	}
}
