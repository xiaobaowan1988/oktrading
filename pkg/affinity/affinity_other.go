//go:build !linux

// Package affinity exposes CPU-pinning primitives.
// On non-Linux platforms these are no-ops.
package affinity

import "runtime"

// Pin is a no-op on non-Linux platforms.
func Pin(cpu int) error { return nil }

// NumCPU returns runtime.NumCPU on non-Linux platforms.
func NumCPU() int { return runtime.NumCPU() }
