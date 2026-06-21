// Package orderbook implements a price-time priority order book for one symbol.
//
// Design invariants:
//   - NOT thread-safe; must be driven exclusively from the engine goroutine.
//   - Bids are kept sorted descending; asks ascending.  Best bid/ask is [0].
//   - Each price level holds a doubly-linked FIFO list (arrival order = time priority).
//   - A hash map from orderID → (priceNode, list.Element) enables O(1) cancellation.
//   - Price-level slices are kept sorted via binary search; depth is bounded by
//     tick granularity (≪ 10 000 levels in practice), so O(log n) is effectively O(1).
package orderbook

import (
	"container/list"
	"sort"

	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// priceNode holds all resting orders at one price level.
type priceNode struct {
	price    int64
	quantity int64      // sum of Remaining across all orders at this level
	orders   *list.List // elements are *types.Order, front = oldest (time-priority)
}

// orderLoc is the index entry for O(1) cancel.
type orderLoc struct {
	node    *priceNode
	element *list.Element
}

// OrderBook maintains the full resting-order state for one symbol.
type OrderBook struct {
	symbol string

	// Bids sorted descending: best bid = bidPrices[0]
	bidPrices []int64
	bids      map[int64]*priceNode

	// Asks sorted ascending: best ask = askPrices[0]
	askPrices []int64
	asks      map[int64]*priceNode

	// Fast cancel index
	orders map[uint64]*orderLoc
}

// New returns an empty order book for symbol.
func New(symbol string) *OrderBook {
	return &OrderBook{
		symbol:    symbol,
		bidPrices: make([]int64, 0, 64),
		bids:      make(map[int64]*priceNode),
		askPrices: make([]int64, 0, 64),
		asks:      make(map[int64]*priceNode),
		orders:    make(map[uint64]*orderLoc),
	}
}

// Symbol returns the trading pair.
func (ob *OrderBook) Symbol() string { return ob.symbol }

// BestBid returns the highest resting bid price, or 0 if the book is empty.
func (ob *OrderBook) BestBid() int64 {
	if len(ob.bidPrices) == 0 {
		return 0
	}
	return ob.bidPrices[0]
}

// BestAsk returns the lowest resting ask price, or 0 if the book is empty.
func (ob *OrderBook) BestAsk() int64 {
	if len(ob.askPrices) == 0 {
		return 0
	}
	return ob.askPrices[0]
}

// Spread returns the bid-ask spread; 0 if either side is empty.
func (ob *OrderBook) Spread() int64 {
	bid, ask := ob.BestBid(), ob.BestAsk()
	if bid == 0 || ask == 0 {
		return 0
	}
	return ask - bid
}

// Depth returns the number of distinct price levels on each side.
func (ob *OrderBook) Depth() (bids, asks int) {
	return len(ob.bidPrices), len(ob.askPrices)
}

// HasOrder reports whether orderID is currently resting in the book.
func (ob *OrderBook) HasOrder(orderID uint64) bool {
	_, ok := ob.orders[orderID]
	return ok
}

// Match executes a taker order against resting makers using price-time priority.
//
// The taker's Remaining and Status are updated in place.  Filled maker orders
// are removed from the book.  The caller is responsible for post-match
// handling:
//   - Limit/unfilled: call AddOrder to rest the residual.
//   - IOC/unfilled: cancel the residual.
//   - FOK: engine pre-checks liquidity before calling Match.
//   - PostOnly: engine rejects before calling Match if it would cross.
func (ob *OrderBook) Match(taker *types.Order) []*types.Trade {
	var trades []*types.Trade

	switch taker.Side {
	case types.Buy:
		for taker.Remaining > 0 && len(ob.askPrices) > 0 {
			bestAsk := ob.askPrices[0]
			// Limit price check: stop sweeping when best ask exceeds taker's limit.
			if taker.Type == types.Limit && bestAsk > taker.Price {
				break
			}
			node := ob.asks[bestAsk]
			trades = append(trades, ob.fillLevel(taker, node)...)
			if node.quantity == 0 {
				delete(ob.asks, bestAsk)
				ob.askPrices = ob.askPrices[1:]
			}
		}

	case types.Sell:
		for taker.Remaining > 0 && len(ob.bidPrices) > 0 {
			bestBid := ob.bidPrices[0]
			// Limit price check: stop sweeping when best bid falls below taker's limit.
			if taker.Type == types.Limit && bestBid < taker.Price {
				break
			}
			node := ob.bids[bestBid]
			trades = append(trades, ob.fillLevel(taker, node)...)
			if node.quantity == 0 {
				delete(ob.bids, bestBid)
				ob.bidPrices = ob.bidPrices[1:]
			}
		}
	}

	// Update taker status after sweeping.
	switch {
	case taker.Remaining == 0:
		taker.Status = types.StatusFilled
	case taker.FilledQty() > 0:
		taker.Status = types.StatusPartiallyFilled
	}

	return trades
}

// fillLevel consumes as much of taker.Remaining as possible from one price level.
func (ob *OrderBook) fillLevel(taker *types.Order, node *priceNode) []*types.Trade {
	var trades []*types.Trade

	for taker.Remaining > 0 && node.orders.Len() > 0 {
		front := node.orders.Front()
		maker := front.Value.(*types.Order)

		fillQty := min(taker.Remaining, maker.Remaining)

		trades = append(trades, &types.Trade{
			Symbol:     ob.symbol,
			MakerOrder: maker,
			TakerOrder: taker,
			Price:      node.price, // execution at maker's resting price
			Quantity:   fillQty,
			Timestamp:  taker.Timestamp,
		})

		taker.Remaining -= fillQty
		maker.Remaining -= fillQty
		node.quantity -= fillQty

		if maker.Remaining == 0 {
			maker.Status = types.StatusFilled
			node.orders.Remove(front)
			delete(ob.orders, maker.OrderID)
		} else {
			maker.Status = types.StatusPartiallyFilled
		}
	}

	return trades
}

// AddOrder inserts a resting order into the book.  The order must have
// Remaining > 0.  Status is set to Open.
func (ob *OrderBook) AddOrder(order *types.Order) {
	order.Status = types.StatusOpen

	switch order.Side {
	case types.Buy:
		node, exists := ob.bids[order.Price]
		if !exists {
			node = &priceNode{price: order.Price, orders: list.New()}
			ob.bids[order.Price] = node
			ob.insertBidPrice(order.Price)
		}
		elem := node.orders.PushBack(order)
		node.quantity += order.Remaining
		ob.orders[order.OrderID] = &orderLoc{node: node, element: elem}

	case types.Sell:
		node, exists := ob.asks[order.Price]
		if !exists {
			node = &priceNode{price: order.Price, orders: list.New()}
			ob.asks[order.Price] = node
			ob.insertAskPrice(order.Price)
		}
		elem := node.orders.PushBack(order)
		node.quantity += order.Remaining
		ob.orders[order.OrderID] = &orderLoc{node: node, element: elem}
	}
}

// CancelOrder removes an order from the book.
// Returns (order, true) on success; (nil, false) if the order is unknown.
func (ob *OrderBook) CancelOrder(orderID uint64) (*types.Order, bool) {
	loc, ok := ob.orders[orderID]
	if !ok {
		return nil, false
	}

	order := loc.element.Value.(*types.Order)
	loc.node.quantity -= order.Remaining
	loc.node.orders.Remove(loc.element)
	delete(ob.orders, orderID)

	// Prune the price level if it is now empty.
	if loc.node.orders.Len() == 0 {
		if order.Side == types.Buy {
			delete(ob.bids, loc.node.price)
			ob.removeBidPrice(loc.node.price)
		} else {
			delete(ob.asks, loc.node.price)
			ob.removeAskPrice(loc.node.price)
		}
	}

	order.Status = types.StatusCancelled
	return order, true
}

// Snapshot returns a depth snapshot up to maxDepth levels per side.
// Used by the FOK pre-check and market data subsystems.
func (ob *OrderBook) Snapshot(maxDepth int) *types.OrderBookSnapshot {
	snap := &types.OrderBookSnapshot{Symbol: ob.symbol}

	for i, p := range ob.bidPrices {
		if i >= maxDepth {
			break
		}
		n := ob.bids[p]
		snap.Bids = append(snap.Bids, types.PriceLevel{
			Price:    p,
			Quantity: n.quantity,
			Count:    n.orders.Len(),
		})
	}
	for i, p := range ob.askPrices {
		if i >= maxDepth {
			break
		}
		n := ob.asks[p]
		snap.Asks = append(snap.Asks, types.PriceLevel{
			Price:    p,
			Quantity: n.quantity,
			Count:    n.orders.Len(),
		})
	}
	return snap
}

// ── Sorted-slice helpers ──────────────────────────────────────────────────────

// insertBidPrice inserts p into bidPrices, keeping it sorted descending.
func (ob *OrderBook) insertBidPrice(p int64) {
	// Find first index where bidPrices[i] <= p (i.e. where p should go).
	i := sort.Search(len(ob.bidPrices), func(j int) bool {
		return ob.bidPrices[j] <= p
	})
	ob.bidPrices = append(ob.bidPrices, 0)
	copy(ob.bidPrices[i+1:], ob.bidPrices[i:])
	ob.bidPrices[i] = p
}

// insertAskPrice inserts p into askPrices, keeping it sorted ascending.
func (ob *OrderBook) insertAskPrice(p int64) {
	i := sort.Search(len(ob.askPrices), func(j int) bool {
		return ob.askPrices[j] >= p
	})
	ob.askPrices = append(ob.askPrices, 0)
	copy(ob.askPrices[i+1:], ob.askPrices[i:])
	ob.askPrices[i] = p
}

func (ob *OrderBook) removeBidPrice(p int64) {
	i := sort.Search(len(ob.bidPrices), func(j int) bool {
		return ob.bidPrices[j] <= p
	})
	if i < len(ob.bidPrices) && ob.bidPrices[i] == p {
		ob.bidPrices = append(ob.bidPrices[:i], ob.bidPrices[i+1:]...)
	}
}

func (ob *OrderBook) removeAskPrice(p int64) {
	i := sort.Search(len(ob.askPrices), func(j int) bool {
		return ob.askPrices[j] >= p
	})
	if i < len(ob.askPrices) && ob.askPrices[i] == p {
		ob.askPrices = append(ob.askPrices[:i], ob.askPrices[i+1:]...)
	}
}
