package worker

/*
#include "glue.h"
*/
import "C"

import (
	"encoding/json"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/mirzomumin/balius/internal/metrics"
)

// Message is what a worker tells the supervisor on its IPC pipe, one JSON
// document per line.
type Message struct {
	// Metrics is a snapshot of the worker's counters.
	Metrics *metrics.Snapshot `json:"metrics,omitempty"`
	// Heartbeat is a sign of life of the worker's Python interpreter.
	Heartbeat bool `json:"heartbeat,omitempty"`
	// Recycle asks the supervisor to gracefully replace the worker.
	Recycle bool `json:"recycle,omitempty"`
}

// ipcChannel serializes the messages of the goroutines reporting.
type ipcChannel struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func newIPCChannel() *ipcChannel {
	return &ipcChannel{enc: json.NewEncoder(os.NewFile(FdIPC, "balius-ipc"))}
}

// send reports false once the supervisor is gone.
func (c *ipcChannel) send(m Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enc.Encode(m) == nil
}

func (w *Worker) reportMetrics() {
	tick := time.NewTicker(time.Duration(max(1, w.cfg.MetricsScrapeInterval)) * time.Second)
	defer tick.Stop()
	send := func() bool {
		if w.loop != nil {
			w.metrics.BlockingQueue.Store(w.loop.pending.Load())
			w.metrics.BlockingBusy.Store(w.loop.busy.Load())
		}
		snap := w.metrics.Snapshot()
		return w.ipc.send(Message{Metrics: &snap})
	}
	for {
		select {
		case <-tick.C:
			if !send() {
				return
			}
		case <-w.stopped:
			send()
			return
		}
	}
}

// heartbeat is the worker's health signal.
//
// Each beat is sent after taking (and releasing) the GIL from a thread of
// its own, so it proves Python code can run: a worker whose interpreter is
// stuck (a C extension that never releases the GIL, a deadlock, a frozen
// process) goes silent and the supervisor replaces it. A merely busy or
// blocked event loop still beats: slow requests are not a reason to kill a
// worker.
type heartbeat struct {
	stop chan struct{}
	done chan struct{}
}

func (w *Worker) startHeartbeat() {
	hb := &heartbeat{stop: make(chan struct{}), done: make(chan struct{})}
	w.heartbeat = hb
	// several beats per timeout, so that one late beat doesn't kill a worker
	interval := min(time.Second, time.Duration(w.cfg.WorkersHealthcheckTimeout)*time.Second/4)
	// the application is loaded: from now on silence means trouble, even if
	// the interpreter freezes before the first tick
	w.ipc.send(Message{Heartbeat: true})
	go func() {
		defer close(hb.done)
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-hb.stop:
				return
			case <-tick.C:
			}
			C.PyGILState_Release(C.PyGILState_Ensure())
			if !w.ipc.send(Message{Heartbeat: true}) {
				return
			}
		}
	}()
}

// stopHeartbeat must be called with the GIL held, before the interpreter is
// finalized: the GIL is released while waiting, as the heartbeat thread may
// be queued for it.
func (w *Worker) stopHeartbeat() {
	if w.heartbeat == nil {
		return
	}
	close(w.heartbeat.stop)
	state := C.PyEval_SaveThread()
	<-w.heartbeat.done
	C.PyEval_RestoreThread(state)
}
