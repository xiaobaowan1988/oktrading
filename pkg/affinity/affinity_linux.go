//go:build linux

// Package affinity exposes CPU-pinning primitives backed by Linux
// sched_setaffinity(2).  Pinning the engine goroutine to a dedicated core
// eliminates OS scheduler jitter and reduces cache-miss latency by keeping
// the hot data in that core's L1/L2.
package affinity

/*
#define _GNU_SOURCE
#include <sched.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

// pin_thread pins the calling thread to cpu using sched_setaffinity.
// Returns 0 on success, errno on failure.
static int pin_thread(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &set) != 0) {
        return errno;
    }
    return 0;
}

// num_cpu returns the number of online processors via sysconf.
static int num_cpu(void) {
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    if (n <= 0) return 1;
    return (int)n;
}
*/
import "C"
import (
	"fmt"
	"syscall"
)

// Pin pins the calling OS thread to the given cpu index.
// Must be called from a goroutine that has been locked to its OS thread via
// runtime.LockOSThread.
func Pin(cpu int) error {
	rc := C.pin_thread(C.int(cpu))
	if rc != 0 {
		return fmt.Errorf("sched_setaffinity(cpu=%d): %w", cpu, syscall.Errno(rc))
	}
	return nil
}

// NumCPU returns the number of online logical CPUs as reported by the OS.
func NumCPU() int {
	return int(C.num_cpu())
}
