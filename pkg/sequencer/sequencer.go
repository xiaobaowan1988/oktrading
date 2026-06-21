// Package sequencer provides a globally monotonic sequence-number generator.
//
// In production each symbol engine maintains its own local sequence for intra-
// symbol ordering.  This package offers a cross-symbol global counter used by
// the gateway to stamp incoming commands before dispatch, providing a total
// order over all symbols for audit logs and disaster-recovery replay.
package sequencer

import "sync/atomic"

// Sequencer is a thread-safe, monotonically increasing counter.
// The zero value is ready to use.
type Sequencer struct {
	_ [56]byte     // pad: prevent false sharing with adjacent allocations
	n atomic.Uint64
	_ [56]byte
}

// New returns a new Sequencer starting at 1.
func New() *Sequencer { return &Sequencer{} }

// Next returns the next unique sequence number.  Sequence numbers start at 1;
// zero is reserved as "unassigned".
func (s *Sequencer) Next() uint64 {
	return s.n.Add(1)
}

// Peek returns the most recently issued sequence number without advancing it.
func (s *Sequencer) Peek() uint64 {
	return s.n.Load()
}
