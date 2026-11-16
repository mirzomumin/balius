// Package metrics holds the runtime counters of balius workers and renders
// them in the Prometheus text exposition format.
package metrics

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Prefix is prepended to every exported metric name.
const Prefix = "balius_"

// Worker is the set of live counters of one worker process.
type Worker struct {
	ConnActive       atomic.Int64
	ConnHandled      atomic.Int64
	ConnErr          atomic.Int64
	ReqHandled       atomic.Int64
	ReqStaticHandled atomic.Int64
	ReqStaticErr     atomic.Int64
	BlockingThreads  atomic.Int64
	BlockingQueue    atomic.Int64
	// Cumulative times, in microseconds.
	BlockingIdle atomic.Int64
	BlockingBusy atomic.Int64
	PyWait       atomic.Int64

	birth time.Time
}

// NewWorker returns zeroed worker counters.
func NewWorker() *Worker { return &Worker{birth: time.Now()} }

// Snapshot is a point-in-time copy of a worker's counters. The order of the
// values matches workerMetrics.
type Snapshot [len(workerMetrics)]int64

// Snapshot captures the current values.
func (w *Worker) Snapshot() Snapshot {
	return Snapshot{
		int64(time.Since(w.birth) / time.Second),
		w.ConnActive.Load(),
		w.ConnHandled.Load(),
		w.ConnErr.Load(),
		w.ReqHandled.Load(),
		w.ReqStaticHandled.Load(),
		w.ReqStaticErr.Load(),
		w.BlockingThreads.Load(),
		w.BlockingQueue.Load(),
		w.BlockingIdle.Load(),
		w.BlockingBusy.Load(),
		w.PyWait.Load(),
	}
}

var workerMetrics = [...]struct{ name, kind string }{
	{"worker_lifetime", "counter"},
	{"connections_active", "gauge"},
	{"connections_handled", "counter"},
	{"connections_err", "counter"},
	{"requests_handled", "counter"},
	{"static_requests_handled", "counter"},
	{"static_requests_err", "counter"},
	{"blocking_threads", "gauge"},
	{"blocking_queue", "gauge"},
	{"blocking_idle_cumulative", "counter"},
	{"blocking_busy_cumulative", "counter"},
	{"py_wait_cumulative", "counter"},
}

// Aggregator collects the snapshots of every worker along with the
// supervisor's own counters.
type Aggregator struct {
	Spawns      atomic.Int64
	RespawnsErr atomic.Int64
	RespawnsTTL atomic.Int64
	RespawnsRSS atomic.Int64

	mu      sync.Mutex
	workers []*Snapshot
}

// NewAggregator returns an aggregator for n workers.
func NewAggregator(n int) *Aggregator { return &Aggregator{workers: make([]*Snapshot, n)} }

// Collect stores the latest snapshot of the worker at index idx (0-based).
func (a *Aggregator) Collect(idx int, s Snapshot) {
	a.mu.Lock()
	if idx >= 0 && idx < len(a.workers) {
		a.workers[idx] = &s
	}
	a.mu.Unlock()
}

// Render formats every metric in the Prometheus text format.
func (a *Aggregator) Render() string {
	var b strings.Builder
	main := [...]struct {
		name string
		v    int64
	}{
		{"workers_spawns", a.Spawns.Load()},
		{"workers_respawns_for_err", a.RespawnsErr.Load()},
		{"workers_respawns_for_lifetime", a.RespawnsTTL.Load()},
		{"workers_respawns_for_rss", a.RespawnsRSS.Load()},
	}
	for _, m := range main {
		fmt.Fprintf(&b, "# TYPE %s%s counter\n%s%s %d\n", Prefix, m.name, Prefix, m.name, m.v)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, m := range workerMetrics {
		fmt.Fprintf(&b, "# TYPE %s%s %s\n", Prefix, m.name, m.kind)
		for idx, snap := range a.workers {
			if snap == nil {
				continue
			}
			fmt.Fprintf(&b, "%s%s{worker=\"%d\"} %d\n", Prefix, m.name, idx+1, snap[i])
		}
	}
	return b.String()
}
