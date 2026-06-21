// Package types defines the core domain types for the matching engine.
// All prices and quantities use a fixed-point representation with 8 decimal
// places (ScaleFactor = 1e8).  For example, 50 000.12345678 BTC/USDT is
// stored as the int64 value 5_000_012_345_678.
package types

// ScaleFactor is the fixed-point multiplier for all price and quantity fields.
const ScaleFactor = 1_0000_0000 // 1e8

// Side identifies whether an order is a buy or a sell.
type Side int8

const (
	Buy  Side = 1
	Sell Side = -1
)

func (s Side) String() string {
	if s == Buy {
		return "BUY"
	}
	return "SELL"
}

// OrderType controls matching behaviour.
type OrderType int8

const (
	Limit    OrderType = 1 // rest in book if not immediately matchable
	Market   OrderType = 2 // fill at best available price; cancel residual
	IOC      OrderType = 3 // Immediate Or Cancel – fill what crosses, cancel rest
	FOK      OrderType = 4 // Fill Or Kill – fill completely or reject
	PostOnly OrderType = 5 // Maker-only – reject if it would match immediately
)

// OrderStatus tracks the lifecycle of an order.
type OrderStatus int8

const (
	StatusNew             OrderStatus = 1
	StatusOpen            OrderStatus = 2
	StatusPartiallyFilled OrderStatus = 3
	StatusFilled          OrderStatus = 4
	StatusCancelled       OrderStatus = 5
	StatusRejected        OrderStatus = 6
)

func (s OrderStatus) String() string {
	switch s {
	case StatusNew:
		return "NEW"
	case StatusOpen:
		return "OPEN"
	case StatusPartiallyFilled:
		return "PARTIALLY_FILLED"
	case StatusFilled:
		return "FILLED"
	case StatusCancelled:
		return "CANCELLED"
	case StatusRejected:
		return "REJECTED"
	}
	return "UNKNOWN"
}

// Order is the central entity. Remaining is decremented as fills occur.
// All monetary fields are fixed-point (×ScaleFactor).
type Order struct {
	OrderID    uint64
	ClientOID  string
	Symbol     string
	UserID     uint64
	Side       Side
	Type       OrderType
	Price      int64       // 0 for Market orders
	Quantity   int64       // original quantity
	Remaining  int64       // unfilled quantity; updated in place during matching
	Status     OrderStatus
	SequenceNo uint64      // assigned by the engine before processing
	Timestamp  int64       // Unix nanoseconds; set by gateway if zero
}

// FilledQty returns the total quantity that has been executed.
func (o *Order) FilledQty() int64 {
	return o.Quantity - o.Remaining
}

// Trade represents one execution event between a maker and a taker.
// Price is always the maker's resting price (price-improvement goes to taker).
type Trade struct {
	TradeID    uint64
	Symbol     string
	MakerOrder *Order
	TakerOrder *Order
	Price      int64  // execution price – the maker's limit price
	Quantity   int64  // matched quantity
	SequenceNo uint64
	Timestamp  int64  // Unix nanoseconds
}

// CommandType selects the engine operation.
type CommandType int8

const (
	CmdNewOrder    CommandType = 1
	CmdCancelOrder CommandType = 2
)

// Command is the message dispatched to a matching engine instance.
// SequenceNo is stamped by the engine; callers leave it as 0.
type Command struct {
	Type       CommandType
	Order      *Order // set for CmdNewOrder
	CancelID   uint64 // set for CmdCancelOrder
	UserID     uint64 // used for cancel authorisation
	SequenceNo uint64 // set by engine
}

// EventType identifies the outcome published on the outbound ring buffer.
type EventType int8

const (
	EvtOrderAccepted  EventType = 1
	EvtOrderFilled    EventType = 2 // partial or full fill
	EvtOrderCancelled EventType = 3
	EvtOrderRejected  EventType = 4
	EvtTrade          EventType = 5
)

// Event is published by the engine for every state transition.
// Downstream consumers (risk engine, persistence, market data) fan out from here.
type Event struct {
	Type       EventType
	SequenceNo uint64
	Symbol     string
	Order      *Order // non-nil for order lifecycle events
	Trade      *Trade // non-nil for EvtTrade
	Reason     string // human-readable rejection reason
}

// PriceLevel is one row in an order book depth snapshot.
type PriceLevel struct {
	Price    int64
	Quantity int64
	Count    int // number of resting orders
}

// OrderBookSnapshot is a point-in-time depth view used for FOK pre-checks
// and market data dissemination.
type OrderBookSnapshot struct {
	Symbol     string
	Bids       []PriceLevel // sorted descending by price
	Asks       []PriceLevel // sorted ascending by price
	SequenceNo uint64
	Timestamp  int64
}
