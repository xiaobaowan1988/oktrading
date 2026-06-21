// Package shard implements symbol-level sharding for the matching engine cluster.
//
// Each trading pair (symbol) is assigned exactly one Engine instance.  The
// Manager creates engines on demand and routes commands to the correct shard.
// This mirrors OKX's "symbol sharding" architecture where every instrument has
// its own isolated state machine – failures and hot spots in one symbol cannot
// affect others.
//
// Reads of the engine map are lock-free (sync.Map); writes (new symbol
// registration) are infrequent and use the map's internal compare-and-store.
package shard

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/xiaobaowan1988/oktrading/pkg/engine"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// Manager routes commands to per-symbol engine instances.
type Manager struct {
	engines sync.Map // map[string]*engine.Engine

	// Configuration for newly created engines.
	inboundCap  int
	outboundCap int

	count atomic.Int64 // total active engines
}

// NewManager creates a Manager.  cap values are forwarded to each engine's
// ring buffer constructor (0 = engine defaults).
func NewManager(inboundCap, outboundCap int) *Manager {
	return &Manager{
		inboundCap:  inboundCap,
		outboundCap: outboundCap,
	}
}

// GetOrCreate returns the engine for symbol, creating and starting it if it
// does not yet exist.  Safe for concurrent callers.
func (m *Manager) GetOrCreate(symbol string) *engine.Engine {
	if v, ok := m.engines.Load(symbol); ok {
		return v.(*engine.Engine)
	}

	// Create a new engine and race to register it.
	candidate := engine.New(symbol, m.inboundCap, m.outboundCap)
	actual, loaded := m.engines.LoadOrStore(symbol, candidate)
	if !loaded {
		// We won the race – start the engine we just stored.
		candidate.Start()
		m.count.Add(1)
		return candidate
	}
	// Another goroutine registered a different engine; discard ours.
	return actual.(*engine.Engine)
}

// Get returns the engine for symbol and a boolean indicating whether it exists.
func (m *Manager) Get(symbol string) (*engine.Engine, bool) {
	v, ok := m.engines.Load(symbol)
	if !ok {
		return nil, false
	}
	return v.(*engine.Engine), true
}

// Submit routes cmd to the correct engine shard.
// Returns an error if the symbol has no engine or the inbound buffer is full.
func (m *Manager) Submit(cmd *types.Command) error {
	var symbol string
	if cmd.Order != nil {
		symbol = cmd.Order.Symbol
	} else {
		// For cancel commands the caller must set the symbol explicitly via
		// the wrapper below, or route via the engine directly.
		return fmt.Errorf("shard: cannot determine symbol for cancel command")
	}

	eng := m.GetOrCreate(symbol)
	if !eng.TrySubmit(cmd) {
		return fmt.Errorf("shard: inbound buffer full for symbol %s", symbol)
	}
	return nil
}

// SubmitToSymbol routes cmd to the engine for symbol (bypassing the auto-
// create path).  Useful for cancel commands where the symbol is known.
func (m *Manager) SubmitToSymbol(symbol string, cmd *types.Command) error {
	eng, ok := m.Get(symbol)
	if !ok {
		return fmt.Errorf("shard: no engine for symbol %s", symbol)
	}
	if !eng.TrySubmit(cmd) {
		return fmt.Errorf("shard: inbound buffer full for symbol %s", symbol)
	}
	return nil
}

// StopAll signals every engine to stop and waits for all to exit.
func (m *Manager) StopAll() {
	var wg sync.WaitGroup
	m.engines.Range(func(_, v any) bool {
		eng := v.(*engine.Engine)
		wg.Add(1)
		go func() {
			defer wg.Done()
			eng.Stop()
		}()
		return true
	})
	wg.Wait()
}

// ActiveCount returns the number of currently running engine shards.
func (m *Manager) ActiveCount() int64 {
	return m.count.Load()
}

// Symbols returns a snapshot of all registered symbol names.
func (m *Manager) Symbols() []string {
	var out []string
	m.engines.Range(func(k, _ any) bool {
		out = append(out, k.(string))
		return true
	})
	return out
}
