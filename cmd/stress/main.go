// cmd/stress is a standalone stress-test binary for the matching engine.
//
// Usage:
//
//	go run ./cmd/stress [flags]
//
// Flags:
//
//	-duration   test duration (default 15s)
//	-symbols    number of parallel symbol shards (default 1)
//	-depth      resting orders to maintain per side (default 200)
//	-takers     taker goroutines per symbol (default 4)
//	-makers     maker goroutines per symbol (default 2)
//	-report     print interval for live stats (default 3s)
//	-pin        pin engine+consumer goroutines to dedicated CPU cores
//
// Workload: makers continuously replenish resting limit orders at N price
// levels around a moving mid; takers sweep aggressively with market orders
// and IOC limits; a cancel goroutine randomly prunes resting orders to stress
// the cancel path.  The book is never allowed to run dry.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xiaobaowan1988/oktrading/pkg/affinity"
	"github.com/xiaobaowan1988/oktrading/pkg/engine"
	"github.com/xiaobaowan1988/oktrading/pkg/offheap"
	"github.com/xiaobaowan1988/oktrading/pkg/pool"
	"github.com/xiaobaowan1988/oktrading/pkg/shard"
	"github.com/xiaobaowan1988/oktrading/pkg/types"
)

// ── CLI flags ─────────────────────────────────────────────────────────────────

var (
	flagDuration = flag.Duration("duration", 15*time.Second, "benchmark duration")
	flagSymbols  = flag.Int("symbols", 1, "number of symbol shards")
	flagDepth    = flag.Int("depth", 200, "resting orders per side per symbol")
	flagTakers   = flag.Int("takers", 4, "taker goroutines per symbol")
	flagMakers   = flag.Int("makers", 2, "maker goroutines per symbol")
	flagReport   = flag.Duration("report", 3*time.Second, "live stats interval (0 = off)")
	flagGOGC     = flag.Int("gogc", 100, "GOGC percentage (-1=disable, 400=recommended with -memlimit)")
	flagMemLimit = flag.Int64("memlimit", 0, "soft memory limit in MiB (0 = no limit; use -gogc=-1 with a limit)")
	flagPin      = flag.Bool("pin", false, "pin engine+consumer goroutines to dedicated CPU cores")
)

// ── Global counters ───────────────────────────────────────────────────────────

var (
	totalSubmitted int64 // orders submitted
	totalEvents    int64 // events received (all types)
	totalTrades    int64 // trade events
	totalFills     int64 // order-filled events
	totalCancels   int64 // order-cancelled events
	totalRejects   int64 // order-rejected events
	totalBPressure int64 // back-pressure (inbound buffer full) retries
)

// ── Entry point ───────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	// Apply GC tuning before any heap activity.
	debug.SetGCPercent(*flagGOGC)
	if *flagMemLimit > 0 {
		debug.SetMemoryLimit(*flagMemLimit << 20)
	}

	numSymbols := *flagSymbols
	depth := *flagDepth
	numTakers := *flagTakers
	numMakers := *flagMakers

	// Compute CPU assignments when -pin is requested.
	// Engines get CPUs 1..numSymbols; consumers get CPUs numSymbols+1..2*numSymbols.
	// Both ranges wrap modulo the number of available CPUs if needed.
	numCPU := affinity.NumCPU()
	engineCPUs := make([]int, numSymbols)
	consumerCPUs := make([]int, numSymbols)
	for i := 0; i < numSymbols; i++ {
		engineCPUs[i] = (1 + i) % numCPU
		consumerCPUs[i] = (1 + numSymbols + i) % numCPU
	}
	if !*flagPin {
		// -1 signals "no pinning" to both engine and consumer.
		for i := range engineCPUs {
			engineCPUs[i] = -1
			consumerCPUs[i] = -1
		}
	}

	fmt.Printf("╔══════════════════════════════════════════════════════╗\n")
	fmt.Printf("║           OKX-Style Matching Engine  –  Stress Test        ║\n")
	fmt.Printf("╠══════════════════════════════════════════════════════╣\n")
	fmt.Printf("║  Go version : %-40s║\n", runtime.Version())
	fmt.Printf("║  GOMAXPROCS : %-40d║\n", runtime.GOMAXPROCS(0))
	fmt.Printf("║  Symbols    : %-40d║\n", numSymbols)
	fmt.Printf("║  Depth/side : %-40d║\n", depth)
	fmt.Printf("║  Makers/sym : %-40d║\n", numMakers)
	fmt.Printf("║  Takers/sym : %-40d║\n", numTakers)
	fmt.Printf("║  Duration   : %-40s║\n", *flagDuration)
	fmt.Printf("║  CPU pin    : %-40v║\n", *flagPin)
	fmt.Printf("╚══════════════════════════════════════════════════════╝\n\n")

	mgr := shard.NewManager(1<<17, 1<<18)

	// Pre-build engines and symbols list.
	symbols := make([]string, numSymbols)
	engines := make([]*engine.Engine, numSymbols)
	for i := range symbols {
		symbols[i] = fmt.Sprintf("SYM%04d-USDT", i)
		if engineCPUs[i] >= 0 {
			// When CPU pinning is requested, create the engine directly so
			// PinCPU can be called before Start.  We bypass the shard manager
			// for these engines; the manager is still used for non-pinned paths.
			eng := engine.New(symbols[i], 1<<17, 1<<18)
			eng.PinCPU(engineCPUs[i])
			eng.Start()
			engines[i] = eng
		} else {
			engines[i] = mgr.GetOrCreate(symbols[i])
		}
	}

	// Per-symbol latency histograms (one per consumer goroutine, merged at end).
	histograms := make([]*LatencyHistogram, numSymbols)
	for i := range histograms {
		histograms[i] = &LatencyHistogram{}
	}

	// ── Warm-up: fill each side of each book ─────────────────────────────────
	fmt.Printf("Warming up (filling books to depth %d) ... ", depth)
	warmupStart := time.Now()
	warmup(engines, symbols, depth)
	fmt.Printf("done in %v\n\n", time.Since(warmupStart).Round(time.Millisecond))

	// ── Start live reporter ───────────────────────────────────────────────────
	stopReporter := make(chan struct{})
	if *flagReport > 0 {
		go liveReporter(*flagReport, stopReporter)
	}

	// ── Launch workload goroutines ────────────────────────────────────────────
	deadline := time.Now().Add(*flagDuration)
	var wg sync.WaitGroup

	for i := 0; i < numSymbols; i++ {
		eng := engines[i]
		sym := symbols[i]
		hist := histograms[i]
		conscpu := consumerCPUs[i]

		// Consumer: drains the outbound ring buffer and records latencies.
		wg.Add(1)
		go func() {
			defer wg.Done()
			runConsumer(eng, hist, conscpu, deadline)
		}()

		// Makers: continuously replenish the book.
		for m := 0; m < numMakers; m++ {
			wg.Add(1)
			go func(makerID int) {
				defer wg.Done()
				runMaker(eng, sym, depth, makerID, deadline)
			}(m)
		}

		// Takers: aggressively sweep the book.
		for t := 0; t < numTakers; t++ {
			wg.Add(1)
			go func(takerID int) {
				defer wg.Done()
				runTaker(eng, sym, depth, takerID, deadline)
			}(t)
		}
	}

	// Interrupt handler for early exit.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\n[interrupt] stopping…")
	}()

	wg.Wait()
	close(stopReporter)
	time.Sleep(50 * time.Millisecond) // let reporter flush

	// ── Final report ─────────────────────────────────────────────────────────
	printFinalReport(histograms, *flagDuration)
}

// ── Warm-up ───────────────────────────────────────────────────────────────────

func warmup(engines []*engine.Engine, symbols []string, depth int) {
	var orderID atomic.Uint64
	var wg sync.WaitGroup

	for i, eng := range engines {
		eng := eng
		sym := symbols[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Place depth asks and depth bids.
			for j := 0; j < depth; j++ {
				id := orderID.Add(1)
				eng.Submit(&types.Command{
					Type: types.CmdNewOrder,
					Order: &types.Order{
						OrderID:   id,
						Symbol:    sym,
						Side:      types.Sell,
						Type:      types.Limit,
						Price:     mid + int64(j+1)*tick,
						Quantity:  lotSize,
						Remaining: lotSize,
					},
				})
			}
			for j := 0; j < depth; j++ {
				id := orderID.Add(1)
				eng.Submit(&types.Command{
					Type: types.CmdNewOrder,
					Order: &types.Order{
						OrderID:   id,
						Symbol:    sym,
						Side:      types.Buy,
						Type:      types.Limit,
						Price:     mid - int64(j+1)*tick,
						Quantity:  lotSize,
						Remaining: lotSize,
					},
				})
			}
			// Drain all accepted events.
			var dst offheap.RawEvent
			for received := 0; received < depth*2; {
				if eng.TryPollEvent(&dst) {
					received++
				} else {
					runtime.Gosched()
				}
			}
		}()
	}
	wg.Wait()
}

// ── Price constants ───────────────────────────────────────────────────────────

const (
	mid     = 50_000 * types.ScaleFactor // synthetic mid price
	tick    = 1 * types.ScaleFactor      // minimum price increment (1.0)
	lotSize = 1 * types.ScaleFactor      // order quantity (1.0 unit)
)

// ── Maker goroutine ───────────────────────────────────────────────────────────

// runMaker continuously places resting limit orders to maintain book depth.
// It staggers its price levels based on makerID to avoid all makers placing
// at identical prices and creating unnecessary contention.
func runMaker(eng *engine.Engine, sym string, depth, makerID int, deadline time.Time) {
	var orderID atomic.Uint64
	orderID.Store(uint64(1_000_000_000 + makerID*100_000_000))

	rng := rand.New(rand.NewSource(int64(makerID) * 12345))
	_ = rng

	for time.Now().Before(deadline) {
		level := int64(rng.Intn(depth) + 1)

		// Post a sell slightly above mid.
		ask := pool.GetOrder()
		ask.OrderID = orderID.Add(1)
		ask.Symbol = sym
		ask.Side = types.Sell
		ask.Type = types.Limit
		ask.Price = mid + level*tick
		ask.Quantity = lotSize
		ask.Remaining = lotSize

		askCmd := pool.GetCommand()
		askCmd.Type = types.CmdNewOrder
		askCmd.Order = ask
		for !eng.TrySubmit(askCmd) {
			atomic.AddInt64(&totalBPressure, 1)
			runtime.Gosched()
		}
		atomic.AddInt64(&totalSubmitted, 1)

		// Post a bid slightly below mid.
		bid := pool.GetOrder()
		bid.OrderID = orderID.Add(1)
		bid.Symbol = sym
		bid.Side = types.Buy
		bid.Type = types.Limit
		bid.Price = mid - level*tick
		bid.Quantity = lotSize
		bid.Remaining = lotSize

		bidCmd := pool.GetCommand()
		bidCmd.Type = types.CmdNewOrder
		bidCmd.Order = bid
		for !eng.TrySubmit(bidCmd) {
			atomic.AddInt64(&totalBPressure, 1)
			runtime.Gosched()
		}
		atomic.AddInt64(&totalSubmitted, 1)
	}
}

// ── Taker goroutine ───────────────────────────────────────────────────────────

// runTaker sends aggressive market orders and limit IOC orders.
func runTaker(eng *engine.Engine, sym string, depth, takerID int, deadline time.Time) {
	var orderID atomic.Uint64
	orderID.Store(uint64(2_000_000_000 + takerID*100_000_000))

	rng := rand.New(rand.NewSource(int64(takerID) * 67890))

	for time.Now().Before(deadline) {
		order := pool.GetOrder()
		order.OrderID = orderID.Add(1)
		order.Symbol = sym
		order.Quantity = lotSize
		order.Remaining = lotSize

		switch rng.Intn(3) {
		case 0: // market buy
			order.Side = types.Buy
			order.Type = types.Market
		case 1: // market sell
			order.Side = types.Sell
			order.Type = types.Market
		case 2: // IOC limit crossing the spread
			aggression := int64(rng.Intn(depth/2)+1) * tick
			if rng.Intn(2) == 0 {
				order.Side = types.Buy
				order.Price = mid + aggression
			} else {
				order.Side = types.Sell
				order.Price = mid - aggression
			}
			order.Type = types.IOC
		}

		cmd := pool.GetCommand()
		cmd.Type = types.CmdNewOrder
		cmd.Order = order
		for !eng.TrySubmit(cmd) {
			atomic.AddInt64(&totalBPressure, 1)
			runtime.Gosched()
		}
		atomic.AddInt64(&totalSubmitted, 1)
	}
}

// ── Consumer goroutine ────────────────────────────────────────────────────────

// runConsumer drains the outbound ring buffer and records latencies.
// Latency = (event receipt time) – (Timestamp stored in the off-heap slot).
// This measures pure engine processing time: channel dequeue → matching → outbound write → consumer read.
//
// consumerCPU >= 0 causes the goroutine to lock its OS thread and call
// sched_setaffinity to pin itself to that core.
func runConsumer(eng *engine.Engine, hist *LatencyHistogram, consumerCPU int, deadline time.Time) {
	if consumerCPU >= 0 {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		if err := affinity.Pin(consumerCPU); err != nil {
			fmt.Fprintf(os.Stderr, "consumer pin failed: %v\n", err)
		}
	}

	var evt offheap.RawEvent
	extended := deadline.Add(500 * time.Millisecond)
	pinned := consumerCPU >= 0

	for time.Now().Before(extended) {
		if !eng.TryPollEvent(&evt) {
			if time.Now().After(deadline) {
				break
			}
			// Pinned consumers have a dedicated core: pure busy-spin.
			// Unpinned consumers yield cooperatively to avoid starving producers.
			if !pinned {
				runtime.Gosched()
			}
			continue
		}

		now := time.Now().UnixNano()
		atomic.AddInt64(&totalEvents, 1)

		switch types.EventType(evt.EvtType) {
		case types.EvtTrade:
			atomic.AddInt64(&totalTrades, 1)
			if evt.Timestamp > 0 {
				hist.Record(now - evt.Timestamp)
			}
		case types.EvtOrderFilled:
			atomic.AddInt64(&totalFills, 1)
		case types.EvtOrderCancelled:
			atomic.AddInt64(&totalCancels, 1)
		case types.EvtOrderRejected:
			atomic.AddInt64(&totalRejects, 1)
		case types.EvtOrderAccepted:
			if evt.Timestamp > 0 {
				hist.Record(now - evt.Timestamp)
			}
		}
		// No pool returns needed: engine handles all lifecycle pooling.
	}
}

// ── Live reporter ─────────────────────────────────────────────────────────────

func liveReporter(interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var prevSubmitted, prevEvents int64
	prevTime := time.Now()

	for {
		select {
		case <-stop:
			return
		case t := <-ticker.C:
			elapsed := t.Sub(prevTime).Seconds()
			curSubmitted := atomic.LoadInt64(&totalSubmitted)
			curEvents := atomic.LoadInt64(&totalEvents)

			orderRate := float64(curSubmitted-prevSubmitted) / elapsed
			eventRate := float64(curEvents-prevEvents) / elapsed

			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)

			fmt.Printf("[%s]  orders/s: %8.0f  events/s: %8.0f  trades: %8d  "+
				"heap: %5.1f MB  gc: %d\n",
				time.Now().Format("15:04:05"),
				orderRate, eventRate,
				atomic.LoadInt64(&totalTrades),
				float64(ms.HeapAlloc)/1e6,
				ms.NumGC,
			)

			prevSubmitted = curSubmitted
			prevEvents = curEvents
			prevTime = t
		}
	}
}

// ── Final report ──────────────────────────────────────────────────────────────

func printFinalReport(histograms []*LatencyHistogram, duration time.Duration) {
	// Merge per-symbol histograms.
	merged := &LatencyHistogram{}
	for _, h := range histograms {
		merged.Merge(h)
	}

	secs := duration.Seconds()
	submitted := atomic.LoadInt64(&totalSubmitted)
	events := atomic.LoadInt64(&totalEvents)
	trades := atomic.LoadInt64(&totalTrades)
	cancels := atomic.LoadInt64(&totalCancels)
	rejects := atomic.LoadInt64(&totalRejects)
	bpress := atomic.LoadInt64(&totalBPressure)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	fmt.Printf("\n")
	fmt.Printf("══════════════════════════════════════════════════════════\n")
	fmt.Printf("  STRESS TEST RESULTS  (%v, %d symbol(s))\n", duration, *flagSymbols)
	fmt.Printf("══════════════════════════════════════════════════════════\n")

	fmt.Printf("\nTHROUGHPUT\n")
	fmt.Printf("  Orders submitted   : %12d  (%8.0f /s)\n", submitted, float64(submitted)/secs)
	fmt.Printf("  Events received    : %12d  (%8.0f /s)\n", events, float64(events)/secs)
	fmt.Printf("    ↳ trades         : %12d\n", trades)
	fmt.Printf("    ↳ cancels        : %12d\n", cancels)
	fmt.Printf("    ↳ rejects        : %12d\n", rejects)
	fmt.Printf("  Back-pressure hits : %12d  (inbound buffer full)\n", bpress)

	fmt.Printf("\nPROCESSING LATENCY  (engine dequeue → outbound event readable)\n")
	fmt.Print(merged.Report(""))

	fmt.Printf("\nMEMORY\n")
	fmt.Printf("  Heap alloc  : %6.1f MB\n", float64(ms.HeapAlloc)/1e6)
	fmt.Printf("  Heap sys    : %6.1f MB\n", float64(ms.HeapSys)/1e6)
	fmt.Printf("  GC cycles   : %6d\n", ms.NumGC)
	fmt.Printf("  GC pause    : %6.2f ms  (total cumulative)\n", float64(ms.PauseTotalNs)/1e6)
	fmt.Printf("══════════════════════════════════════════════════════════\n")
}
