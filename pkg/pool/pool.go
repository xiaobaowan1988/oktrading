// Package pool provides sync.Pool instances for the hottest allocation types.
//
// Lifecycle rules (must be respected by all callers):
//
//	Command  – returned to pool by the engine after process() completes.
//	Event    – returned to pool by the consumer after it has finished reading
//	           all fields (including nested Order/Trade pointers).
//	Trade    – returned to pool by the consumer after processing EvtTrade.
//	Order    – returned to pool by the consumer ONLY for terminal states:
//	           StatusFilled, StatusCancelled, StatusRejected.
//	           EvtOrderAccepted must NOT be returned (order still lives in book).
package pool

import (
	"sync"

	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

var (
	cmdPool   = sync.Pool{New: func() any { return new(types.Command) }}
	orderPool = sync.Pool{New: func() any { return new(types.Order) }}
	eventPool = sync.Pool{New: func() any { return new(types.Event) }}
	tradePool = sync.Pool{New: func() any { return new(types.Trade) }}
)

// GetCommand returns a zeroed Command from the pool.
func GetCommand() *types.Command { return cmdPool.Get().(*types.Command) }

// PutCommand zeros c and returns it to the pool.
func PutCommand(c *types.Command) { *c = types.Command{}; cmdPool.Put(c) }

// GetOrder returns a zeroed Order from the pool.
func GetOrder() *types.Order { return orderPool.Get().(*types.Order) }

// PutOrder zeros o and returns it to the pool.
func PutOrder(o *types.Order) { *o = types.Order{}; orderPool.Put(o) }

// GetEvent returns a zeroed Event from the pool.
func GetEvent() *types.Event { return eventPool.Get().(*types.Event) }

// PutEvent zeros evt and returns it to the pool.
func PutEvent(evt *types.Event) { *evt = types.Event{}; eventPool.Put(evt) }

// GetTrade returns a zeroed Trade from the pool.
func GetTrade() *types.Trade { return tradePool.Get().(*types.Trade) }

// PutTrade zeros t and returns it to the pool.
func PutTrade(t *types.Trade) { *t = types.Trade{}; tradePool.Put(t) }
