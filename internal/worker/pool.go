package worker

/*
#include "glue.h"
*/
import "C"

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzomumin/tulpar/internal/metrics"
)

// pyPool is the pool of Python threads running WSGI applications.
//
// Each member is a goroutine pinned to its own OS thread, holding a Python
// thread state for its whole life; it takes the GIL only while running a
// task. The first thread lives forever, the others are spawned on demand
// (up to max) and retire after idling for idleTimeout.
type pyPool struct {
	tasks       chan poolTask
	max         int32
	idleTimeout time.Duration

	threads atomic.Int32

	m    *metrics.Worker
	quit chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

func newPyPool(maxThreads int, idleTimeout time.Duration, m *metrics.Worker) *pyPool {
	return &pyPool{
		tasks:       make(chan poolTask),
		max:         int32(maxThreads),
		idleTimeout: idleTimeout,
		m:           m,
		quit:        make(chan struct{}),
	}
}

// spawnDelay is how long a task waits for a busy pool before the pool
// considers growing.
//
// A new thread only helps when the existing ones are busy away from the
// interpreter, typically blocked on I/O with the GIL released. When threads
// are instead queued for the GIL, the interpreter is the bottleneck and one
// more thread would only add to the queue, so the pool doesn't grow then.
const spawnDelay = time.Millisecond

// poolTask runs on a pool thread, WITHOUT the GIL: it gets the thread's
// Python state and must take and release the GIL within a single C call.
// Taking the GIL in one cgo call and using it in the next would leave it
// held while the thread waits for a Go scheduler slot in between, stalling
// every other Python thread.
type poolTask func(ts *C.PyThreadState)

// run hands a task to a Python thread, blocking until one takes it. It
// reports false when the pool is stopped.
func (p *pyPool) run(task poolTask) bool {
	p.once.Do(func() { p.spawn(true) })
	p.m.BlockingQueue.Add(1)
	defer p.m.BlockingQueue.Add(-1)

	select {
	case p.tasks <- task:
		return true
	default:
	}
	timer := time.NewTimer(spawnDelay)
	defer timer.Stop()
	for {
		select {
		case p.tasks <- task:
			return true
		case <-p.quit:
			return false
		case <-timer.C:
			if C.tulpar_gil_waiters() == 0 {
				if n := p.threads.Load(); n < p.max && p.threads.CompareAndSwap(n, n+1) {
					p.spawn(false)
				}
			}
			timer.Reset(spawnDelay)
		}
	}
}

func (p *pyPool) spawn(primary bool) {
	if primary {
		p.threads.Add(1)
	}
	p.wg.Add(1)
	p.m.BlockingThreads.Add(1)
	go p.thread(primary)
}

func (p *pyPool) thread(primary bool) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer p.wg.Done()
	defer p.m.BlockingThreads.Add(-1)
	defer p.threads.Add(-1)

	// Register this OS thread with the interpreter, then park without the
	// GIL until there is work.
	gil := C.PyGILState_Ensure()
	state := C.PyEval_SaveThread()
	defer func() {
		C.PyEval_RestoreThread(state)
		C.PyGILState_Release(gil)
	}()

	var timer *time.Timer
	var timeout <-chan time.Time
	if !primary {
		timer = time.NewTimer(p.idleTimeout)
		defer timer.Stop()
		timeout = timer.C
	}
	for {
		idleStart := time.Now()
		var task poolTask
		select {
		case task = <-p.tasks:
		case <-timeout:
		case <-p.quit:
		}
		p.m.BlockingIdle.Add(int64(time.Since(idleStart) / time.Microsecond))
		if task == nil {
			return
		}
		busyStart := time.Now()
		task(state)
		p.m.BlockingBusy.Add(int64(time.Since(busyStart) / time.Microsecond))

		if timer != nil {
			timer.Reset(p.idleTimeout) // since Go 1.23 this drops a stale expiry
		}
	}
}

// stop retires every thread. It must be called without the GIL.
func (p *pyPool) stop() {
	close(p.quit)
	p.wg.Wait()
}
