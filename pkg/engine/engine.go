// Package engine implements the per-symbol deterministic matching engine.
//
// # Architecture
//
//	Gateway / maker / taker goroutines  (multiple concurrent producers)
//	     │  (TrySubmit / Submit)
//	     ▼
//	┌─────────────────────────────────────────────────┐
//	│  inbound  *mpsc.Ring  (lock-free CAS ring)       │
//	│           Producers claim slots via CAS; writes  │
//	│           to different slots happen in parallel. │
//	└───────────────────┬─────────────────────────────┘
//	                    │  receive
//	                    ▼
//	         ┌──────────────────────┐
//	         │  Engine goroutine    │  ← runtime.LockOSThread() + optional sched_setaffinity
//	         │  (state machine)     │
//	         │  ┌────────────────┐  │
//	         │  │  OrderBook     │  │
//	         │  │  (in-memory)   │  │
//	         │  └────────────────┘  │
//	         └──────────┬───────────┘
//	                    │  PushOrder / PushTrade (single producer)
//	                    ▼
//	┌─────────────────────────────────────────────────┐
//	│  outbound EventRing  (SPSC, C-malloc'd, off-heap)│
//	└─────────────────────────────────────────────────┘
//	     │  (TryPollEvent)
//	     ▼
//	Result handler / persistence / market-data fan-out
//
// # Off-heap outbound path
//
// Events are written directly into C-malloc'd memory (pkg/offheap).  The Go
// garbage collector never sees these allocations, so GC write-barriers on the
// outbound hot path are eliminated entirely.  This removes the primary source
// of GC-induced tail latency in the event pipeline.
//
// # CPU Affinity
//
// Call PinCPU(n) before Start() to have the engine goroutine call
// sched_setaffinity(0, {n}) after locking its OS thread.  This eliminates OS
// scheduler jitter and maximises L1/L2 cache locality for the order-book state.
//
// # Determinism
//
// Every command received from the inbound channel is stamped with a
// monotonically increasing SequenceNo before processing.  Given the same
// sequence of commands a standby replica reproduces the exact same order
// book state — enabling hot standby failover with minimal recovery time.
package engine

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/xiaobaowan1988/oktrading/pkg/affinity"
	"github.com/xiaobaowan1988/oktrading/pkg/mpsc"
	"github.com/xiaobaowan1988/oktrading/pkg/offheap"
	"github.com/xiaobaowan1988/oktrading/pkg/orderbook"
	"github.com/xiaobaowan1988/oktrading/pkg/pool"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

const (
	defaultInboundSize  = 1 << 14 // 16 384 slots
	defaultOutboundSize = 1 << 15 // 32 768 slots (trades + order updates can burst)
)

// Engine is a deterministic matching engine for exactly one symbol.
// It must be started with Start() before commands are submitted.
type Engine struct {
	symbol      string
	book        *orderbook.OrderBook
	localSeq    uint64 // intra-symbol sequence; monotone, no gaps
	tradeSeq    uint64 // trade ID counter
	cpuAffinity int    // -1 means no pinning

	// inbound is a lock-free MPSC ring — producers claim slots via CAS.
	inbound *mpsc.Ring
	// outbound is a SPSC ring buffer backed by C-malloc'd memory.
	outbound *offheap.EventRing

	quit chan struct{}
	done chan struct{}
}

// New creates an Engine for symbol.  inboundCap sets the channel buffer size;
// outboundCap sets the SPSC ring buffer capacity (rounded up to power of two).
// Pass 0 to use the defaults.
func New(symbol string, inboundCap, outboundCap int) *Engine {
	if inboundCap <= 0 {
		inboundCap = defaultInboundSize
	}
	if outboundCap <= 0 {
		outboundCap = defaultOutboundSize
	}
	return &Engine{
		symbol:      symbol,
		book:        orderbook.New(symbol),
		inbound:     mpsc.New(inboundCap),
		outbound:    offheap.New(outboundCap),
		cpuAffinity: -1,
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
	}
}

// Symbol returns the trading pair this engine processes.
func (e *Engine) Symbol() string { return e.symbol }

// PinCPU configures the engine goroutine to be pinned to the given CPU core
// via sched_setaffinity.  Must be called before Start().
func (e *Engine) PinCPU(cpu int) {
	e.cpuAffinity = cpu
}

// Start launches the engine's matching goroutine.
func (e *Engine) Start() {
	go e.run()
}

// Stop signals the engine to finish and waits for it to exit.
func (e *Engine) Stop() {
	close(e.quit)
	<-e.done
}

// TrySubmit dispatches a command without blocking.
// Returns false if the inbound ring is at capacity (back-pressure signal).
func (e *Engine) TrySubmit(cmd *types.Command) bool {
	return e.inbound.TrySubmit(cmd)
}

// Submit dispatches a command, spinning until the inbound ring has space.
func (e *Engine) Submit(cmd *types.Command) {
	e.inbound.Submit(cmd)
}

// TryPollEvent reads one result event from the outbound ring buffer into dst.
// Returns false when the ring is empty.
func (e *Engine) TryPollEvent(dst *offheap.RawEvent) bool {
	return e.outbound.TryPop(dst)
}

// ── Matching loop ─────────────────────────────────────────────────────────────

func (e *Engine) run() {
	// Locking the goroutine to its OS thread is required before calling
	// sched_setaffinity, which pins the *thread* not the goroutine.
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		close(e.done)
	}()

	if e.cpuAffinity >= 0 {
		if err := affinity.Pin(e.cpuAffinity); err != nil {
			// Graceful degradation: log the failure but continue running.
			fmt.Fprintf(os.Stderr, "engine %s: cpu pin failed: %v\n", e.symbol, err)
		}
	}

	pinned := e.cpuAffinity >= 0
	idle := 0
	for {
		cmd := e.inbound.TryPop()
		if cmd != nil {
			idle = 0
			e.process(cmd)
			continue
		}
		idle++
		// Check quit signal every 16k idle iterations (~16 µs at 1 GHz spin).
		if idle&0x3FFF == 0 {
			select {
			case <-e.quit:
				return
			default:
			}
		}
		// Yield cooperatively when not pinned to avoid burning an unshared core.
		if !pinned {
			runtime.Gosched()
		}
	}
}

func (e *Engine) process(cmd *types.Command) {
	// Stamp the global sequence number onto the command.  This is the single
	// point at which ordering is established — determinism guarantee.
	e.localSeq++
	cmd.SequenceNo = e.localSeq

	switch cmd.Type {
	case types.CmdNewOrder:
		e.handleNewOrder(cmd)
	case types.CmdCancelOrder:
		e.handleCancelOrder(cmd)
	}
	// Command is no longer needed after handlers complete; return to pool.
	cmd.Order = nil
	pool.PutCommand(cmd)
}

func (e *Engine) handleNewOrder(cmd *types.Command) {
	order := cmd.Order
	order.SequenceNo = cmd.SequenceNo
	if order.Timestamp == 0 {
		order.Timestamp = time.Now().UnixNano()
	}

	switch order.Type {

	case types.PostOnly:
		// Reject before touching the book if it would cross immediately.
		if e.wouldCross(order) {
			order.Status = types.StatusRejected
			e.outbound.PushOrderWithReason(types.EvtOrderRejected, e.localSeq, order, "post-only order would immediately match")
			pool.PutOrder(order)
			return
		}
		e.book.AddOrder(order)
		e.outbound.PushOrder(types.EvtOrderAccepted, e.localSeq, order)
		// order now rests in the book — do NOT pool

	case types.FOK:
		// Pre-check: if full fill is impossible, reject without touching the book.
		if !e.canFillCompletely(order) {
			order.Status = types.StatusRejected
			e.outbound.PushOrderWithReason(types.EvtOrderRejected, e.localSeq, order, "insufficient liquidity for FOK")
			pool.PutOrder(order)
			return
		}
		// Full fill is guaranteed – proceed.
		trades := e.book.Match(order)
		e.emitTrades(trades)
		e.emitOrderUpdate(order)
		pool.PutOrder(order)

	case types.Limit:
		trades := e.book.Match(order)
		e.emitTrades(trades)
		if order.Remaining > 0 {
			// Rest the unfilled portion as a maker order.
			e.book.AddOrder(order)
			e.outbound.PushOrder(types.EvtOrderAccepted, e.localSeq, order)
			// order rests in book — do NOT pool
		} else {
			e.emitOrderUpdate(order)
			pool.PutOrder(order)
		}

	case types.IOC:
		trades := e.book.Match(order)
		e.emitTrades(trades)
		if order.Remaining > 0 {
			// Cancel whatever could not be immediately filled.
			order.Status = types.StatusCancelled
			e.outbound.PushOrder(types.EvtOrderCancelled, e.localSeq, order)
			pool.PutOrder(order)
		} else {
			e.emitOrderUpdate(order)
			pool.PutOrder(order)
		}

	case types.Market:
		trades := e.book.Match(order)
		e.emitTrades(trades)
		if order.Remaining > 0 {
			// Market order exhausted all available liquidity; cancel residual.
			order.Status = types.StatusCancelled
			e.outbound.PushOrder(types.EvtOrderCancelled, e.localSeq, order)
			pool.PutOrder(order)
		} else {
			e.emitOrderUpdate(order)
			pool.PutOrder(order)
		}

	default:
		order.Status = types.StatusRejected
		e.outbound.PushOrderWithReason(types.EvtOrderRejected, e.localSeq, order, "unknown order type")
		pool.PutOrder(order)
	}
}

func (e *Engine) handleCancelOrder(cmd *types.Command) {
	order, ok := e.book.CancelOrder(cmd.CancelID)
	if !ok {
		e.outbound.PushOrderWithReason(types.EvtOrderRejected, e.localSeq, nil, "cancel rejected: order not found")
		return
	}
	e.outbound.PushOrder(types.EvtOrderCancelled, e.localSeq, order)
	pool.PutOrder(order)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// wouldCross returns true if order would immediately match against resting orders.
// Used for PostOnly pre-check.
func (e *Engine) wouldCross(order *types.Order) bool {
	switch order.Side {
	case types.Buy:
		best := e.book.BestAsk()
		return best > 0 && order.Price >= best
	case types.Sell:
		best := e.book.BestBid()
		return best > 0 && order.Price <= best
	}
	return false
}

// canFillCompletely scans a book snapshot to determine whether enough
// crossable liquidity exists to fill the FOK order in full.
// This is a read-only operation; it does not modify the order book.
func (e *Engine) canFillCompletely(order *types.Order) bool {
	snap := e.book.Snapshot(4096) // deep scan
	remaining := order.Remaining

	switch order.Side {
	case types.Buy:
		for _, lvl := range snap.Asks {
			if order.Type == types.Limit && lvl.Price > order.Price {
				break
			}
			if lvl.Quantity >= remaining {
				return true
			}
			remaining -= lvl.Quantity
		}
	case types.Sell:
		for _, lvl := range snap.Bids {
			if order.Type == types.Limit && lvl.Price < order.Price {
				break
			}
			if lvl.Quantity >= remaining {
				return true
			}
			remaining -= lvl.Quantity
		}
	}
	return remaining == 0
}

func (e *Engine) emitTrades(trades []*types.Trade) {
	for _, tr := range trades {
		e.tradeSeq++
		tr.TradeID = e.tradeSeq

		makerID := uint64(0)
		makerStatus := types.OrderStatus(0)
		if tr.MakerOrder != nil {
			makerID = tr.MakerOrder.OrderID
			makerStatus = tr.MakerOrder.Status
		}
		e.outbound.PushTrade(e.localSeq, tr.TradeID, tr.TakerOrder, makerID, makerStatus, tr.Price, tr.Quantity)

		// Return fully-consumed maker to pool (engine now owns lifecycle).
		if tr.MakerOrder != nil && tr.MakerOrder.Status == types.StatusFilled {
			pool.PutOrder(tr.MakerOrder)
		}
		pool.PutTrade(tr)
	}
}

func (e *Engine) emitOrderUpdate(order *types.Order) {
	e.outbound.PushOrder(types.EvtOrderFilled, e.localSeq, order)
}
