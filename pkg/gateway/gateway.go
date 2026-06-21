// Package gateway implements the order entry gateway – the first layer that
// all client requests pass through before reaching the matching engine.
//
// Responsibilities (layered, keeping the matching hot-path clean):
//
//  1. Parameter validation – reject malformed requests early.
//  2. Per-user rate limiting – token-bucket algorithm, configurable RPS.
//  3. Order-ID assignment – atomic global counter ensures uniqueness.
//  4. Symbol routing – dispatch to the correct engine shard via the Manager.
//
// What the gateway deliberately does NOT do:
//   - Margin / balance checks (handled by a dedicated risk engine that reads
//     the order book snapshots asynchronously).
//   - Persistence (done downstream by the event pipeline consuming outbound
//     ring buffers).
package gateway

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xiaobaowan1988/oktrading/pkg/shard"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// Sentinel errors returned by PlaceOrder and CancelOrder.
var (
	ErrRateLimited     = errors.New("gateway: rate limit exceeded")
	ErrInvalidOrder    = errors.New("gateway: invalid order parameters")
	ErrBufferFull      = errors.New("gateway: engine inbound buffer full")
	ErrSymbolNotFound  = errors.New("gateway: symbol not found")
)

// Config holds gateway-wide tunables.
type Config struct {
	// MaxOrdersPerSecond is the per-user rate limit for new orders.
	// 0 disables rate limiting (useful for benchmarks / tests).
	MaxOrdersPerSecond int
}

// DefaultConfig returns production-safe defaults.
func DefaultConfig() Config {
	return Config{MaxOrdersPerSecond: 100}
}

// Gateway is the single entry point for all order operations.
type Gateway struct {
	cfg      Config
	manager  *shard.Manager
	orderSeq atomic.Uint64 // global order-ID generator

	rlMu     sync.Mutex
	rateLims map[uint64]*tokenBucket
}

// New creates a Gateway backed by the given shard manager.
func New(manager *shard.Manager, cfg Config) *Gateway {
	return &Gateway{
		cfg:      cfg,
		manager:  manager,
		rateLims: make(map[uint64]*tokenBucket),
	}
}

// PlaceOrder validates, rate-limits, stamps, and dispatches a new order.
// The order's OrderID is set by the gateway; callers should leave it as 0.
func (g *Gateway) PlaceOrder(order *types.Order) error {
	if err := validateOrder(order); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidOrder, err)
	}

	if g.cfg.MaxOrdersPerSecond > 0 && !g.allowUser(order.UserID) {
		return ErrRateLimited
	}

	// Assign a globally unique order ID.
	order.OrderID = g.orderSeq.Add(1)
	if order.Timestamp == 0 {
		order.Timestamp = time.Now().UnixNano()
	}

	cmd := &types.Command{
		Type:  types.CmdNewOrder,
		Order: order,
	}

	eng := g.manager.GetOrCreate(order.Symbol)
	if !eng.TrySubmit(cmd) {
		return ErrBufferFull
	}
	return nil
}

// CancelOrder dispatches a cancel command for the given order ID.
// symbol must be the same symbol the order was placed on.
func (g *Gateway) CancelOrder(userID uint64, symbol string, orderID uint64) error {
	if symbol == "" || orderID == 0 {
		return ErrInvalidOrder
	}

	cmd := &types.Command{
		Type:     types.CmdCancelOrder,
		CancelID: orderID,
		UserID:   userID,
	}
	if err := g.manager.SubmitToSymbol(symbol, cmd); err != nil {
		return fmt.Errorf("%w: %v", ErrSymbolNotFound, err)
	}
	return nil
}

// ── Order validation ──────────────────────────────────────────────────────────

func validateOrder(o *types.Order) error {
	if o.Symbol == "" {
		return errors.New("symbol is empty")
	}
	if o.Side != types.Buy && o.Side != types.Sell {
		return fmt.Errorf("invalid side %d", o.Side)
	}
	if o.Quantity <= 0 {
		return errors.New("quantity must be positive")
	}
	switch o.Type {
	case types.Limit, types.PostOnly, types.IOC, types.FOK:
		if o.Price <= 0 {
			return errors.New("limit/post-only/IOC/FOK orders require a positive price")
		}
	case types.Market:
		// no price required
	default:
		return fmt.Errorf("unknown order type %d", o.Type)
	}
	return nil
}

// ── Token-bucket rate limiter (per user) ─────────────────────────────────────

type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	maxTok   float64
	refill   float64 // tokens per nanosecond
	lastTime int64   // unix nanos of last refill
}

func newTokenBucket(rps int) *tokenBucket {
	max := float64(rps)
	return &tokenBucket{
		tokens:   max,
		maxTok:   max,
		refill:   max / 1e9, // convert RPS → tokens/ns
		lastTime: time.Now().UnixNano(),
	}
}

func (tb *tokenBucket) allow() bool {
	now := time.Now().UnixNano()
	tb.mu.Lock()
	defer tb.mu.Unlock()

	elapsed := float64(now - tb.lastTime)
	tb.lastTime = now
	tb.tokens += elapsed * tb.refill
	if tb.tokens > tb.maxTok {
		tb.tokens = tb.maxTok
	}
	if tb.tokens < 1 {
		return false
	}
	tb.tokens--
	return true
}

// allowUser checks and consumes one token from the user's rate bucket.
func (g *Gateway) allowUser(userID uint64) bool {
	g.rlMu.Lock()
	tb, ok := g.rateLims[userID]
	if !ok {
		tb = newTokenBucket(g.cfg.MaxOrdersPerSecond)
		g.rateLims[userID] = tb
	}
	g.rlMu.Unlock()
	return tb.allow()
}
