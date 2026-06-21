// cmd/engine demonstrates the end-to-end flow:
//
//   Gateway → Shard Manager → Engine (per symbol) → Event consumer
//
// Two symbols run in parallel on independent engine goroutines, showing the
// sharding isolation property.  A "market-maker" goroutine posts resting
// limit orders; a "taker" goroutine sweeps them with market orders.  A
// consumer goroutine drains the outbound ring buffers and prints events.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/xiaobaowan1988/oktrading/pkg/gateway"
	"github.com/xiaobaowan1988/oktrading/pkg/offheap"
	"github.com/xiaobaowan1988/oktrading/pkg/shard"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

const (
	scaleFactor = types.ScaleFactor

	// Two symbols run on independent shards simultaneously.
	symBTC = "BTC-USDT"
	symETH = "ETH-USDT"

	makerUserID = 1001
	takerUserID = 1002
)

func main() {
	mgr := shard.NewManager(0, 0)
	gw := gateway.New(mgr, gateway.DefaultConfig())

	// Pre-create engines so the consumer can poll them immediately.
	btcEng := mgr.GetOrCreate(symBTC)
	ethEng := mgr.GetOrCreate(symETH)

	var wg sync.WaitGroup

	// ── Consumer: drain both outbound ring buffers ──────────────────────────
	wg.Add(1)
	go func() {
		defer wg.Done()
		deadline := time.Now().Add(5 * time.Second)
		var evt offheap.RawEvent
		for time.Now().Before(deadline) {
			// BTC shard
			for {
				if !btcEng.TryPollEvent(&evt) {
					break
				}
				printEvent(symBTC, &evt)
			}
			// ETH shard
			for {
				if !ethEng.TryPollEvent(&evt) {
					break
				}
				printEvent(symETH, &evt)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// ── Market maker: post resting limit orders on both symbols ────────────
	wg.Add(1)
	go func() {
		defer wg.Done()

		btcOffers := []struct{ price, qty int64 }{
			{50_000 * scaleFactor, 2 * scaleFactor},
			{50_100 * scaleFactor, 3 * scaleFactor},
			{49_900 * scaleFactor, 1 * scaleFactor},
		}
		ethOffers := []struct{ price, qty int64 }{
			{3_000 * scaleFactor, 5 * scaleFactor},
			{3_010 * scaleFactor, 2 * scaleFactor},
		}

		for _, o := range btcOffers {
			placeLimit(gw, symBTC, makerUserID, types.Sell, o.price, o.qty)
			time.Sleep(50 * time.Millisecond)
		}
		for _, o := range ethOffers {
			placeLimit(gw, symETH, makerUserID, types.Sell, o.price, o.qty)
			time.Sleep(50 * time.Millisecond)
		}

		// Also post some bids so we can demo a spread.
		placeLimit(gw, symBTC, makerUserID, types.Buy, 49_800*scaleFactor, scaleFactor)
		placeLimit(gw, symETH, makerUserID, types.Buy, 2_990*scaleFactor, 3*scaleFactor)
	}()

	// ── Taker: place aggressive orders after a short delay ──────────────────
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond) // let maker orders settle

		// BTC market buy – sweeps the cheapest asks.
		placeMarket(gw, symBTC, takerUserID, types.Buy, 4*scaleFactor)
		time.Sleep(100 * time.Millisecond)

		// ETH IOC – fills what it can, cancels the rest.
		placeIOC(gw, symETH, takerUserID, types.Buy, 3_000*scaleFactor, 10*scaleFactor)
		time.Sleep(100 * time.Millisecond)

		// BTC FOK – try to buy 100 BTC (not enough liquidity → rejected).
		placeFOK(gw, symBTC, takerUserID, types.Buy, 51_000*scaleFactor, 100*scaleFactor)
		time.Sleep(100 * time.Millisecond)

		// BTC PostOnly sell – would cross the resting bid → rejected.
		placePostOnly(gw, symBTC, takerUserID, types.Sell, 49_800*scaleFactor, scaleFactor)
	}()

	// ── Graceful shutdown ────────────────────────────────────────────────────
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sig:
		fmt.Println("\n[shutdown] signal received")
	case <-time.After(5 * time.Second):
		fmt.Println("\n[shutdown] demo complete")
	}

	mgr.StopAll()
	wg.Wait()

	fmt.Printf("\nActive shards at exit: %d\n", mgr.ActiveCount())
}

// ── Order helper functions ────────────────────────────────────────────────────

func placeLimit(gw *gateway.Gateway, sym string, uid uint64, side types.Side, price, qty int64) {
	err := gw.PlaceOrder(&types.Order{
		Symbol:    sym,
		UserID:    uid,
		Side:      side,
		Type:      types.Limit,
		Price:     price,
		Quantity:  qty,
		Remaining: qty,
	})
	if err != nil {
		fmt.Printf("[gateway] PlaceLimit error: %v\n", err)
	}
}

func placeMarket(gw *gateway.Gateway, sym string, uid uint64, side types.Side, qty int64) {
	err := gw.PlaceOrder(&types.Order{
		Symbol:    sym,
		UserID:    uid,
		Side:      side,
		Type:      types.Market,
		Quantity:  qty,
		Remaining: qty,
	})
	if err != nil {
		fmt.Printf("[gateway] PlaceMarket error: %v\n", err)
	}
}

func placeIOC(gw *gateway.Gateway, sym string, uid uint64, side types.Side, price, qty int64) {
	err := gw.PlaceOrder(&types.Order{
		Symbol:    sym,
		UserID:    uid,
		Side:      side,
		Type:      types.IOC,
		Price:     price,
		Quantity:  qty,
		Remaining: qty,
	})
	if err != nil {
		fmt.Printf("[gateway] PlaceIOC error: %v\n", err)
	}
}

func placeFOK(gw *gateway.Gateway, sym string, uid uint64, side types.Side, price, qty int64) {
	err := gw.PlaceOrder(&types.Order{
		Symbol:    sym,
		UserID:    uid,
		Side:      side,
		Type:      types.FOK,
		Price:     price,
		Quantity:  qty,
		Remaining: qty,
	})
	if err != nil {
		fmt.Printf("[gateway] PlaceFOK error: %v\n", err)
	}
}

func placePostOnly(gw *gateway.Gateway, sym string, uid uint64, side types.Side, price, qty int64) {
	err := gw.PlaceOrder(&types.Order{
		Symbol:    sym,
		UserID:    uid,
		Side:      side,
		Type:      types.PostOnly,
		Price:     price,
		Quantity:  qty,
		Remaining: qty,
	})
	if err != nil {
		fmt.Printf("[gateway] PlacePostOnly error: %v\n", err)
	}
}

// ── Event printer ─────────────────────────────────────────────────────────────

func printEvent(sym string, evt *offheap.RawEvent) {
	const sf = types.ScaleFactor
	switch types.EventType(evt.EvtType) {
	case types.EvtOrderAccepted:
		fmt.Printf("[%s seq=%d] ORDER ACCEPTED  id=%-6d side=%-4s type=%-8s price=%.2f qty=%.8f\n",
			sym, evt.SeqNo, evt.OrderID, sideName(evt.Side),
			orderTypeName(types.OrderType(evt.OrderType)),
			float64(evt.Price)/float64(sf),
			float64(evt.Quantity)/float64(sf),
		)
	case types.EvtTrade:
		fmt.Printf("[%s seq=%d] TRADE          maker=%-6d taker=%-6d price=%.2f qty=%.8f\n",
			sym, evt.SeqNo, evt.MakerOrderID, evt.OrderID,
			float64(evt.Price)/float64(sf),
			float64(evt.Quantity)/float64(sf),
		)
	case types.EvtOrderFilled:
		fmt.Printf("[%s seq=%d] ORDER FILLED   id=%-6d filled=%.8f\n",
			sym, evt.SeqNo, evt.OrderID,
			float64(evt.Quantity-evt.Remaining)/float64(sf),
		)
	case types.EvtOrderCancelled:
		fmt.Printf("[%s seq=%d] ORDER CANCELLED id=%-6d remaining=%.8f\n",
			sym, evt.SeqNo, evt.OrderID,
			float64(evt.Remaining)/float64(sf),
		)
	case types.EvtOrderRejected:
		reason := string(bytes.TrimRight(evt.Reason[:], "\x00"))
		fmt.Printf("[%s seq=%d] ORDER REJECTED  reason=%q\n", sym, evt.SeqNo, reason)
	}
}

func sideName(s int8) string {
	switch types.Side(s) {
	case types.Buy:
		return "BUY"
	case types.Sell:
		return "SELL"
	}
	return "UNKNOWN"
}

func orderTypeName(t types.OrderType) string {
	switch t {
	case types.Limit:
		return "LIMIT"
	case types.Market:
		return "MARKET"
	case types.IOC:
		return "IOC"
	case types.FOK:
		return "FOK"
	case types.PostOnly:
		return "POST_ONLY"
	}
	return "UNKNOWN"
}
