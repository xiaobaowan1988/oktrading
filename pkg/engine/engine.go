// Package engine implements the per-symbol deterministic matching engine.
//
// # Architecture
//
//	Gateway / maker / taker goroutines  (multiple concurrent producers)
//	     │  (TrySubmit / Submit)
//	     ▼
//	┌─────────────────────────────────────────────────┐
//	│  inbound  chan *Command  (buffered Go channel)   │
//	│           MPSC-safe: multiple producers allowed  │
//	└───────────────────┬─────────────────────────────┘
//	                    │  receive
//	                    ▼
//	         ┌──────────────────────┐
//	         │  Engine goroutine    │  ← runtime.LockOSThread()
//	         │  (state machine)     │
//	         │  ┌────────────────┐  │
//	         │  │  OrderBook     │  │
//	         │  │  (in-memory)   │  │
//	         │  └────────────────┘  │
//	         └──────────┬───────────┘
//	                    │  Push (single producer)
//	                    ▼
//	┌─────────────────────────────────────────────────┐
//	│  outbound RingBuffer[Event]    (SPSC, lock-free) │
//	└─────────────────────────────────────────────────┘
//	     │  (TryPollEvent)
//	     ▼
//	Result handler / persistence / market-data fan-out
//
// # Inbound: channel vs ring buffer
//
// The inbound path uses a buffered Go channel rather than the Disruptor ring
// buffer.  The ring buffer is a pure SPSC structure; using it from multiple
// producer goroutines causes a data race where two goroutines read the same
// head value, write to the same slot, then both increment head — leaving a
// slot that was never written and will produce a nil pointer on pop.
//
// A Go channel is internally MPSC-safe and fast enough for the inbound path.
// The single-producer outbound ring buffer (engine → result handler) remains
// as-is and delivers the low-latency lock-free guarantee where it matters.
//
// # Determinism
//
// Every command received from the inbound channel is stamped with a
// monotonically increasing SequenceNo before processing.  Given the same
// sequence of commands a standby replica reproduces the exact same order
// book state — enabling hot standby failover with minimal recovery time.
package engine

import (
	"runtime"
	"time"

	"github.com/xiaobaowan1988/oktrading/pkg/disruptor"
	"github.com/xiaobaowan1988/oktrading/pkg/orderbook"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

const (
	defaultInboundSize  = 1 << 14 // 16 384 slots
	defaultOutboundSize = 1 << 15 // 32 768 slots (trades + order updates can burst)
)

// Engine is a deterministic matching engine for exactly one symbol.
// It must be started with Start() before commands are submitted.
type Engine struct {
	symbol   string
	book     *orderbook.OrderBook
	localSeq uint64 // intra-symbol sequence; monotone, no gaps
	tradeSeq uint64 // trade ID counter

	// inbound is a buffered channel — safe for multiple concurrent producers.
	inbound chan *types.Command
	// outbound is a SPSC ring buffer — written only by the engine goroutine.
	outbound *disruptor.RingBuffer[types.Event]

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
		symbol:   symbol,
		book:     orderbook.New(symbol),
		inbound:  make(chan *types.Command, inboundCap),
		outbound: disruptor.New[types.Event](outboundCap),
		quit:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Symbol returns the trading pair this engine processes.
func (e *Engine) Symbol() string { return e.symbol }

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
// Returns false if the inbound channel is at capacity (back-pressure signal).
func (e *Engine) TrySubmit(cmd *types.Command) bool {
	select {
	case e.inbound <- cmd:
		return true
	default:
		return false
	}
}

// Submit dispatches a command, blocking until the inbound channel has space.
func (e *Engine) Submit(cmd *types.Command) {
	e.inbound <- cmd
}

// TryPollEvent reads one result event from the outbound ring buffer.
// Returns (nil, false) when empty.
func (e *Engine) TryPollEvent() (*types.Event, bool) {
	return e.outbound.TryPop()
}

// ── Matching loop ─────────────────────────────────────────────────────────────

func (e *Engine) run() {
	// Locking the goroutine to its OS thread approximates CPU pinning.
	// For hard affinity combine with sched_setaffinity via cgo.
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
		close(e.done)
	}()

	for {
		select {
		case cmd := <-e.inbound:
			e.process(cmd)
		case <-e.quit:
			return
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
			e.emit(&types.Event{
				Type:       types.EvtOrderRejected,
				SequenceNo: e.localSeq,
				Symbol:     e.symbol,
				Order:      order,
				Reason:     "post-only order would immediately match",
			})
			return
		}
		e.book.AddOrder(order)
		e.emit(&types.Event{
			Type:       types.EvtOrderAccepted,
			SequenceNo: e.localSeq,
			Symbol:     e.symbol,
			Order:      order,
		})

	case types.FOK:
		// Pre-check: if full fill is impossible, reject without touching the book.
		if !e.canFillCompletely(order) {
			order.Status = types.StatusRejected
			e.emit(&types.Event{
				Type:       types.EvtOrderRejected,
				SequenceNo: e.localSeq,
				Symbol:     e.symbol,
				Order:      order,
				Reason:     "insufficient liquidity for FOK",
			})
			return
		}
		// Full fill is guaranteed – proceed.
		trades := e.book.Match(order)
		e.emitTrades(trades)
		e.emitOrderUpdate(order)

	case types.Limit:
		trades := e.book.Match(order)
		e.emitTrades(trades)
		if order.Remaining > 0 {
			// Rest the unfilled portion as a maker order.
			e.book.AddOrder(order)
			e.emit(&types.Event{
				Type:       types.EvtOrderAccepted,
				SequenceNo: e.localSeq,
				Symbol:     e.symbol,
				Order:      order,
			})
		} else {
			e.emitOrderUpdate(order)
		}

	case types.IOC:
		trades := e.book.Match(order)
		e.emitTrades(trades)
		if order.Remaining > 0 {
			// Cancel whatever could not be immediately filled.
			order.Status = types.StatusCancelled
			e.emit(&types.Event{
				Type:       types.EvtOrderCancelled,
				SequenceNo: e.localSeq,
				Symbol:     e.symbol,
				Order:      order,
			})
		} else {
			e.emitOrderUpdate(order)
		}

	case types.Market:
		trades := e.book.Match(order)
		e.emitTrades(trades)
		if order.Remaining > 0 {
			// Market order exhausted all available liquidity; cancel residual.
			order.Status = types.StatusCancelled
			e.emit(&types.Event{
				Type:       types.EvtOrderCancelled,
				SequenceNo: e.localSeq,
				Symbol:     e.symbol,
				Order:      order,
			})
		} else {
			e.emitOrderUpdate(order)
		}

	default:
		order.Status = types.StatusRejected
		e.emit(&types.Event{
			Type:       types.EvtOrderRejected,
			SequenceNo: e.localSeq,
			Symbol:     e.symbol,
			Order:      order,
			Reason:     "unknown order type",
		})
	}
}

func (e *Engine) handleCancelOrder(cmd *types.Command) {
	order, ok := e.book.CancelOrder(cmd.CancelID)
	if !ok {
		e.emit(&types.Event{
			Type:       types.EvtOrderRejected,
			SequenceNo: e.localSeq,
			Symbol:     e.symbol,
			Reason:     "cancel rejected: order not found",
		})
		return
	}
	e.emit(&types.Event{
		Type:       types.EvtOrderCancelled,
		SequenceNo: e.localSeq,
		Symbol:     e.symbol,
		Order:      order,
	})
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
		tr.SequenceNo = e.localSeq
		e.emit(&types.Event{
			Type:       types.EvtTrade,
			SequenceNo: e.localSeq,
			Symbol:     e.symbol,
			Trade:      tr,
		})
	}
}

func (e *Engine) emitOrderUpdate(order *types.Order) {
	e.emit(&types.Event{
		Type:       types.EvtOrderFilled,
		SequenceNo: e.localSeq,
		Symbol:     e.symbol,
		Order:      order,
	})
}

func (e *Engine) emit(evt *types.Event) {
	// Push blocks if the outbound buffer is full.  In production the downstream
	// consumer (persistence pipeline) must drain fast enough to avoid this.
	e.outbound.Push(evt)
}
