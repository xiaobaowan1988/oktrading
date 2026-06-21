// Package offheap provides a SPSC ring buffer whose event slots live in
// C-malloc'd memory, entirely off the Go heap.  Because the GC never scans
// these slots, writing to them generates zero write-barrier traffic and
// therefore zero GC tail latency from the hot outbound path.
//
// # Layout
//
// Each slot is a 128-byte RawEvent (two 64-byte cache lines).  The producer
// writes all fields, then advances the head counter with an atomic Add.  The
// consumer observes head > tail, copies the 128-byte slot into its own
// stack-allocated RawEvent, then advances tail.
//
// # Memory-ordering proof
//
// Let P = engine goroutine (producer), C = consumer goroutine.
//
//	P: writes slot fields (C memory, via unsafe.Pointer cast)   [W_data]
//	P: head.Add(1)                                               [W_head]  (seq-cst)
//	C: head.Load() → sees new value                             [R_head]  (seq-cst, syncs-with W_head)
//	C: copies slot into *dst                                     [R_data]
//
// W_data hb W_head  (program order, same goroutine P)
// W_head  sw R_head  (atomic Add → Load of the same location)
// R_head  hb R_data  (program order, same goroutine C)
// ∴ W_data hb R_data — slot is fully visible to the consumer.
package offheap

/*
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

// RawEvent is 128 bytes (two 64-byte cache lines).
// The first cache line holds the hot matching fields; the second holds the
// reason string and padding so the struct fits exactly two cache lines.
typedef struct {
    int8_t   evt_type;        // 1
    int8_t   order_status;    // 1
    int8_t   order_type;      // 1
    int8_t   side;            // 1
    int32_t  _pad0;           // 4  → total 8 bytes so far
    uint64_t seq_no;          // 8  → 16
    uint64_t order_id;        // 8  → 24  taker order ID for trades; order ID otherwise
    uint64_t maker_order_id;  // 8  → 32  maker order ID for trades; 0 otherwise
    uint64_t trade_id;        // 8  → 40
    int64_t  price;           // 8  → 48
    int64_t  quantity;        // 8  → 56
    int64_t  remaining;       // 8  → 64
    int64_t  timestamp;       // 8  → 72  taker.Timestamp for EvtTrade; order.Timestamp otherwise
    char     reason[32];      // 32 → 104
    uint8_t  _pad1[24];       // 24 → 128
} RawEvent;

_Static_assert(sizeof(RawEvent) == 128, "RawEvent must be exactly 128 bytes");

// zero_slot zeroes all 128 bytes of the slot at ptr.
static void zero_slot(void *ptr) {
    memset(ptr, 0, sizeof(RawEvent));
}

// slot_ptr returns a pointer to slot index i within the slab.
static RawEvent* slot_ptr(void *slab, uint64_t i) {
    return ((RawEvent*)slab) + i;
}
*/
import "C"
import (
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// RawEvent mirrors the C RawEvent struct byte-for-byte.
// The init() function below panics if the layouts diverge.
//
// Field mapping when EvtType == EvtTrade:
//   - OrderID      = TakerOrder.OrderID
//   - MakerOrderID = MakerOrder.OrderID
//   - Timestamp    = TakerOrder.Timestamp
//
// For all other event types:
//   - OrderID  = Order.OrderID
//   - Timestamp = Order.Timestamp
type RawEvent struct {
	EvtType      int8
	OrderStatus  int8
	OrderType    int8
	Side         int8
	_            [4]byte  // _pad0
	SeqNo        uint64
	OrderID      uint64
	MakerOrderID uint64
	TradeID      uint64
	Price        int64
	Quantity     int64
	Remaining    int64
	Timestamp    int64
	Reason       [32]byte
	_            [24]byte // _pad1
}

func init() {
	if unsafe.Sizeof(RawEvent{}) != 128 {
		panic("offheap.RawEvent size mismatch: expected 128 bytes")
	}
}

// EventRing is a SPSC ring buffer backed by C-malloc'd memory.
//
// Cache-line layout (64 bytes per line):
//
//	Offset   0: slab     (8 bytes)
//	Offset   8: mask     (8 bytes)
//	Offset  16: pad to 64 bytes (48 bytes)   ← ring header cache line
//	Offset  64: producer.head + 56-byte pad  ← producer cache line
//	Offset 128: consumer.tail + 56-byte pad  ← consumer cache line
type EventRing struct {
	slab unsafe.Pointer // C.malloc'd: capacity * 128 bytes
	mask uint64
	_    [48]byte // pad header to 64 bytes (8+8+48=64)
	producer struct {
		head atomic.Uint64
		_    [56]byte
	}
	consumer struct {
		tail atomic.Uint64
		_    [56]byte
	}
}

// New allocates an EventRing with the given capacity (rounded up to the next
// power of two).  The backing slab is allocated with C.malloc so it is
// invisible to the Go GC.  Panics if malloc fails.
func New(capacity int) *EventRing {
	size := nextPow2(capacity)
	slab := C.malloc(C.size_t(uint64(size) * 128))
	if slab == nil {
		panic("offheap.New: C.malloc returned nil")
	}
	r := &EventRing{
		slab: slab,
		mask: uint64(size - 1),
	}
	return r
}

// Free releases the C-malloc'd slab.  The EventRing must not be used after
// calling Free.
func (r *EventRing) Free() {
	C.free(r.slab)
	r.slab = nil
}

// PushOrder writes an order lifecycle event into the next available slot.
// Blocks (spinning with Gosched) if the ring is full.
func (r *EventRing) PushOrder(evtType types.EventType, seqNo uint64, order *types.Order) {
	slot := r.acquireSlot()
	C.zero_slot(slot)

	s := (*RawEvent)(unsafe.Pointer(slot))
	s.EvtType = int8(evtType)
	s.SeqNo = seqNo
	if order != nil {
		s.OrderStatus = int8(order.Status)
		s.OrderType = int8(order.Type)
		s.Side = int8(order.Side)
		s.OrderID = order.OrderID
		s.Price = order.Price
		s.Quantity = order.Quantity
		s.Remaining = order.Remaining
		s.Timestamp = order.Timestamp
	}

	// Release: make all writes visible before advancing head.
	r.producer.head.Add(1)
}

// PushOrderWithReason writes an order lifecycle event that carries a rejection
// reason string.
func (r *EventRing) PushOrderWithReason(evtType types.EventType, seqNo uint64, order *types.Order, reason string) {
	slot := r.acquireSlot()
	C.zero_slot(slot)

	s := (*RawEvent)(unsafe.Pointer(slot))
	s.EvtType = int8(evtType)
	s.SeqNo = seqNo
	if order != nil {
		s.OrderStatus = int8(order.Status)
		s.OrderType = int8(order.Type)
		s.Side = int8(order.Side)
		s.OrderID = order.OrderID
		s.Price = order.Price
		s.Quantity = order.Quantity
		s.Remaining = order.Remaining
		s.Timestamp = order.Timestamp
	}
	copyReason(&s.Reason, reason)

	r.producer.head.Add(1)
}

// PushTrade writes a trade event into the next available slot.
// Blocks (spinning with Gosched) if the ring is full.
func (r *EventRing) PushTrade(seqNo uint64, tradeID uint64, takerOrder *types.Order, makerOrderID uint64, makerStatus types.OrderStatus, price, qty int64) {
	slot := r.acquireSlot()
	C.zero_slot(slot)

	s := (*RawEvent)(unsafe.Pointer(slot))
	s.EvtType = int8(types.EvtTrade)
	s.SeqNo = seqNo
	s.TradeID = tradeID
	s.MakerOrderID = makerOrderID
	s.OrderStatus = int8(makerStatus)
	s.Price = price
	s.Quantity = qty
	if takerOrder != nil {
		s.OrderID = takerOrder.OrderID
		s.Side = int8(takerOrder.Side)
		s.OrderType = int8(takerOrder.Type)
		s.Remaining = takerOrder.Remaining
		s.Timestamp = takerOrder.Timestamp
	}

	r.producer.head.Add(1)
}

// TryPop copies the next event from the ring into dst.
// Returns false (and leaves dst unchanged) if the ring is empty.
func (r *EventRing) TryPop(dst *RawEvent) bool {
	tail := r.consumer.tail.Load()
	head := r.producer.head.Load() // acquire: synchronizes with producer's Add(1)
	if head == tail {
		return false
	}
	slot := C.slot_ptr(r.slab, C.uint64_t(tail&r.mask))
	// Copy the 128-byte C slot into the Go struct.
	// Both have identical layout (verified by init() and _Static_assert).
	*dst = *(*RawEvent)(unsafe.Pointer(slot))
	r.consumer.tail.Add(1)
	return true
}

// acquireSlot spins until there is space, then returns a pointer to the
// C slot that the producer should write into.  The slot index is the current
// head before the increment.
func (r *EventRing) acquireSlot() unsafe.Pointer {
	for {
		head := r.producer.head.Load()
		tail := r.consumer.tail.Load()
		if head-tail < r.mask+1 {
			// Use the pre-increment head as our slot index.
			return unsafe.Pointer(C.slot_ptr(r.slab, C.uint64_t(head&r.mask)))
		}
		runtime.Gosched()
	}
}

// copyReason copies up to 31 bytes of s into the 32-byte array (null-terminated).
func copyReason(dst *[32]byte, s string) {
	n := len(s)
	if n > 31 {
		n = 31
	}
	copy(dst[:n], s)
	dst[n] = 0
}

// nextPow2 returns the smallest power of two >= n (minimum 2 to avoid mask=0).
func nextPow2(n int) int {
	if n <= 2 {
		return 2
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
