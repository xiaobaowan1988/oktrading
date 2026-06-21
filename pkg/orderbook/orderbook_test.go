package orderbook_test

import (
	"testing"

	"github.com/xiaobaowan1988/oktrading/pkg/orderbook"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// p converts a human-readable price string like "50000" into a fixed-point int64.
// For these tests we work in whole numbers (scale = 1) to keep the values readable.
// The architecture is identical at any scale.
func p(price int64) int64 { return price }
func q(qty int64) int64   { return qty }

func limitSell(id uint64, price, qty int64) *types.Order {
	return &types.Order{
		OrderID:   id,
		Side:      types.Sell,
		Type:      types.Limit,
		Price:     p(price),
		Quantity:  q(qty),
		Remaining: q(qty),
	}
}

func limitBuy(id uint64, price, qty int64) *types.Order {
	return &types.Order{
		OrderID:   id,
		Side:      types.Buy,
		Type:      types.Limit,
		Price:     p(price),
		Quantity:  q(qty),
		Remaining: q(qty),
	}
}

func marketBuy(id uint64, qty int64) *types.Order {
	return &types.Order{
		OrderID:   id,
		Side:      types.Buy,
		Type:      types.Market,
		Quantity:  q(qty),
		Remaining: q(qty),
	}
}

// ── Basic full fill ───────────────────────────────────────────────────────────

func TestFullMatch(t *testing.T) {
	ob := orderbook.New("BTC-USDT")

	sell := limitSell(1, 50000, 1)
	ob.AddOrder(sell)

	buy := limitBuy(2, 50000, 1)
	trades := ob.Match(buy)

	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}
	tr := trades[0]
	if tr.Price != 50000 {
		t.Errorf("trade price: want 50000, got %d", tr.Price)
	}
	if tr.Quantity != 1 {
		t.Errorf("trade qty: want 1, got %d", tr.Quantity)
	}
	if buy.Status != types.StatusFilled {
		t.Errorf("buy status: want Filled, got %v", buy.Status)
	}
	if sell.Status != types.StatusFilled {
		t.Errorf("sell status: want Filled, got %v", sell.Status)
	}

	// Book must be empty.
	bids, asks := ob.Depth()
	if bids != 0 || asks != 0 {
		t.Errorf("book should be empty after full fill, got bids=%d asks=%d", bids, asks)
	}
}

// ── Partial fill ─────────────────────────────────────────────────────────────

func TestPartialFill(t *testing.T) {
	ob := orderbook.New("ETH-USDT")

	sell := limitSell(1, 3000, 10)
	ob.AddOrder(sell)

	buy := limitBuy(2, 3000, 4)
	trades := ob.Match(buy)

	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}
	if trades[0].Quantity != 4 {
		t.Errorf("trade qty: want 4, got %d", trades[0].Quantity)
	}
	if buy.Status != types.StatusFilled {
		t.Error("buy should be fully filled")
	}
	if sell.Remaining != 6 {
		t.Errorf("sell remaining: want 6, got %d", sell.Remaining)
	}
	if sell.Status != types.StatusPartiallyFilled {
		t.Error("sell should be PartiallyFilled")
	}
	if !ob.HasOrder(1) {
		t.Error("sell should still be resting in the book")
	}
}

// ── Price-time priority ───────────────────────────────────────────────────────

func TestPriceTimePriority(t *testing.T) {
	ob := orderbook.New("BTC-USDT")

	// Three sells at different prices – market buy should sweep lowest first.
	ob.AddOrder(limitSell(1, 50100, 1))
	ob.AddOrder(limitSell(2, 50000, 1))
	ob.AddOrder(limitSell(3, 49900, 1))

	if ob.BestAsk() != 49900 {
		t.Errorf("best ask: want 49900, got %d", ob.BestAsk())
	}

	buy := marketBuy(100, 3)
	trades := ob.Match(buy)

	if len(trades) != 3 {
		t.Fatalf("expected 3 trades, got %d", len(trades))
	}
	wantPrices := []int64{49900, 50000, 50100}
	for i, tr := range trades {
		if tr.Price != wantPrices[i] {
			t.Errorf("trade[%d] price: want %d, got %d", i, wantPrices[i], tr.Price)
		}
	}
}

// ── FIFO within the same price level ─────────────────────────────────────────

func TestFIFOWithinLevel(t *testing.T) {
	ob := orderbook.New("BTC-USDT")

	// Two resting asks at the same price – first in, first filled.
	ob.AddOrder(limitSell(1, 50000, 1)) // arrives first
	ob.AddOrder(limitSell(2, 50000, 1)) // arrives second

	buy := limitBuy(10, 50000, 1)
	trades := ob.Match(buy)

	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}
	// Order 1 (first in) must be the maker.
	if trades[0].MakerOrder.OrderID != 1 {
		t.Errorf("maker should be order 1, got %d", trades[0].MakerOrder.OrderID)
	}
	// Order 2 should still be resting.
	if !ob.HasOrder(2) {
		t.Error("order 2 should still be resting")
	}
}

// ── Cancel ───────────────────────────────────────────────────────────────────

func TestCancelOrder(t *testing.T) {
	ob := orderbook.New("BTC-USDT")

	order := limitBuy(42, 45000, 2)
	ob.AddOrder(order)

	cancelled, ok := ob.CancelOrder(42)
	if !ok {
		t.Fatal("cancel should succeed")
	}
	if cancelled.Status != types.StatusCancelled {
		t.Errorf("status: want Cancelled, got %v", cancelled.Status)
	}
	if ob.HasOrder(42) {
		t.Error("order should no longer be in book")
	}

	bids, _ := ob.Depth()
	if bids != 0 {
		t.Error("bid side should be empty after cancel")
	}

	// Double-cancel must return false.
	if _, ok := ob.CancelOrder(42); ok {
		t.Error("second cancel should return false")
	}
}

// ── Limit order does not cross ────────────────────────────────────────────────

func TestLimitNoMatch(t *testing.T) {
	ob := orderbook.New("BTC-USDT")

	ob.AddOrder(limitSell(1, 50100, 1))

	buy := limitBuy(2, 50000, 1) // price below ask – should not match
	trades := ob.Match(buy)

	if len(trades) != 0 {
		t.Fatalf("expected 0 trades, got %d", len(trades))
	}
	if buy.Status == types.StatusFilled || buy.Status == types.StatusPartiallyFilled {
		t.Error("buy should not have been filled")
	}
}

// ── Snapshot ─────────────────────────────────────────────────────────────────

func TestSnapshot(t *testing.T) {
	ob := orderbook.New("BTC-USDT")

	ob.AddOrder(limitBuy(1, 49900, 5))
	ob.AddOrder(limitBuy(2, 49800, 3))
	ob.AddOrder(limitSell(3, 50000, 2))
	ob.AddOrder(limitSell(4, 50100, 4))

	snap := ob.Snapshot(10)

	if len(snap.Bids) != 2 {
		t.Errorf("want 2 bid levels, got %d", len(snap.Bids))
	}
	if len(snap.Asks) != 2 {
		t.Errorf("want 2 ask levels, got %d", len(snap.Asks))
	}
	if snap.Bids[0].Price != 49900 {
		t.Errorf("best bid: want 49900, got %d", snap.Bids[0].Price)
	}
	if snap.Asks[0].Price != 50000 {
		t.Errorf("best ask: want 50000, got %d", snap.Asks[0].Price)
	}
}
