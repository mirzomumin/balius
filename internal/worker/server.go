// Package worker is a balius worker process: an embedded CPython
// interpreter running the application, served by Go's HTTP stack.
//
// Division of labour:
//
//   - goroutines own the network: accepting connections, TLS, HTTP/1 and
//     HTTP/2 framing, websockets, static files;
//   - the process main thread owns the interpreter: on async interfaces
//     (ASGI, RSGI) it runs the asyncio event loop, on WSGI it sleeps while
//     a pool of Python threads runs the application;
//   - the two sides talk through the C glue in this package: Go calls into
//     Python only from the loop thread (or a pool thread), Python calls
//     into Go through the exported go* functions.
package worker

/*
#include "glue.h"
*/
import "C"

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/mirzomumin/balius/internal/config"
	"github.com/mirzomumin/balius/internal/logx"
	"github.com/mirzomumin/balius/internal/metrics"
	"github.com/mirzomumin/balius/internal/netx"
	"github.com/mirzomumin/balius/internal/static"
	"github.com/mirzomumin/balius/internal/tlsx"
)

// File descriptors inherited from the supervisor.
const (
	// FdControl is a pipe: its first line is the Spec (JSON); it then stays
	// open for as long as the supervisor lives.
	FdControl = 3
	// FdIPC is a pipe the worker reports its metrics on.
	FdIPC = 4
	// FdSocket is the shared listening socket, when workers don't open
	// their own.
	FdSocket = 5
)

// Spec is what the supervisor tells a worker at spawn time.
type Spec struct {
	ID           int            `json:"id"`
	Config       *config.Config `json:"config"`
	SharedSocket bool           `json:"shared_socket"`
}

// Worker is the state of a worker process.
type Worker struct {
	id   int
	cfg  *config.Config
	spec *Spec

	loop    *loopBridge // async interfaces
	pool    *pyPool     // WSGI
	metrics *metrics.Worker
	files   *static.Files
	srv     *http.Server
	ws      *wsRegistry
	proxies *trustedProxies // nil: forwarded headers are ignored

	control   *os.File
	ipc       *ipcChannel
	heartbeat *heartbeat
	// maxRequests is the request count at which the worker asks to be
	// recycled (0: never).
	maxRequests int64

	started      bool
	shutdownOnce sync.Once
	stopped      chan struct{} // closed when the server fully stopped
}

// current is the worker of this process.
var current *Worker

// Run is the entry point of a worker process. It must be called from the
// main goroutine, locked to the main OS thread. It returns the exit code.
func Run() int {
	control := os.NewFile(FdControl, "balius-control")
	line, err := bufio.NewReader(control).ReadBytes('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, "balius worker: unable to read the configuration from the supervisor")
		return 2
	}
	spec := &Spec{}
	if err := json.Unmarshal(line, spec); err != nil || spec.Config == nil {
		fmt.Fprintln(os.Stderr, "balius worker: invalid configuration received from the supervisor")
		return 2
	}
	cfg := spec.Config
	logx.Configure(cfg.LogLevel, cfg.LogEnabled)

	w := &Worker{
		id:      spec.ID,
		cfg:     cfg,
		spec:    spec,
		metrics: metrics.NewWorker(),
		files:   static.New(cfg),
		ws:      newWSRegistry(),
		control: control,
		ipc:     newIPCChannel(),
		stopped: make(chan struct{}),
	}
	if cfg.WorkersMaxRequests > 0 {
		w.maxRequests = int64(cfg.WorkersMaxRequests + rand.IntN(cfg.WorkersMaxRequestsJitter+1))
	}
	if cfg.ProxyHeaders {
		w.proxies = parseTrustedProxies(cfg.ForwardedAllowIPs)
	}
	current = w

	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(goProcs(cfg))
	}

	if cfg.Async() {
		if w.loop, err = newLoopBridge(); err != nil {
			logx.Errorf("%v", err)
			return 1
		}
		w.metrics.BlockingThreads.Store(1)
	} else {
		w.pool = newPyPool(cfg.BlockingThreads, time.Duration(cfg.BlockingThreadsIdleTimeout)*time.Second, w.metrics)
	}

	if path := os.Getenv("BALIUS_CPUPROFILE"); path != "" {
		// a profile per worker: <path>.<worker id>
		if f, err := os.Create(fmt.Sprintf("%s.%d", path, w.id)); err == nil {
			if pprof.StartCPUProfile(f) == nil {
				defer func() {
					pprof.StopCPUProfile()
					f.Close()
				}()
			}
		}
	}

	if err := initPython(cfg); err != nil {
		logx.Errorf("%v", err)
		return 1
	}
	cjson := C.CString(string(line))
	code := int(C.balius_py_run_worker(cjson))
	C.free(unsafe.Pointer(cjson))

	if w.started {
		// the application asked to stop on its own (e.g. sys.exit): make
		// sure the network side is down before the interpreter goes away
		w.shutdown()
		<-w.stopped
	}
	w.stopHeartbeat()
	C.balius_py_finalize()
	return code
}

// goProcs sizes the Go scheduler of a worker.
//
// Threads coming back from Python (the event loop, or the WSGI pool
// threads) need a scheduler slot to run Go code, as do the goroutines
// doing network I/O: too few slots and they queue behind each other at
// every crossing. Async workers have a single thread in Python, so a few
// slots beyond the runtime threads saturate them; WSGI pool threads each
// need their own.
func goProcs(cfg *config.Config) int {
	cpus := runtime.NumCPU()
	if !cfg.Async() {
		return cpus
	}
	return min(cpus, max(4, cfg.RuntimeThreads+2))
}

// start brings the network side up. It runs once the application is loaded.
func (w *Worker) start() error {
	listener, err := w.listen()
	if err != nil {
		return err
	}
	w.srv, err = w.newServer()
	if err != nil {
		listener.Close()
		return err
	}
	listener = netx.NewBackpressure(listener, w.cfg.Backpressure, connStats{w.metrics})
	w.started = true

	go func() {
		var err error
		if w.srv.TLSConfig != nil {
			err = w.srv.ServeTLS(listener, "", "")
		} else {
			err = w.srv.Serve(listener)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logx.Errorf("worker-%d server failure: %v", w.id, err)
			w.shutdown()
		}
	}()
	go w.watchSignals()
	go w.watchSupervisor()
	if w.cfg.MetricsEnabled {
		go w.reportMetrics()
	}
	if w.cfg.WorkersHealthcheckTimeout > 0 {
		w.startHeartbeat()
	}
	logx.Infof("Started worker-%d", w.id)
	return nil
}

func (w *Worker) listen() (net.Listener, error) {
	if w.spec.SharedSocket {
		f := os.NewFile(FdSocket, "balius-socket")
		defer f.Close()
		return netx.Listen(f, w.cfg.Backlog)
	}
	return netx.ListenTCP(w.cfg.Host, w.cfg.Port, w.cfg.Backlog)
}

func (w *Worker) newServer() (*http.Server, error) {
	cfg := w.cfg
	protocols := new(http.Protocols)
	switch cfg.HTTP {
	case config.HTTP1:
		protocols.SetHTTP1(true)
	case config.HTTP2:
		protocols.SetHTTP2(true)
		protocols.SetUnencryptedHTTP2(true)
	default:
		protocols.SetHTTP1(true)
		protocols.SetHTTP2(true)
		protocols.SetUnencryptedHTTP2(true)
	}

	srv := &http.Server{
		Handler:           http.HandlerFunc(w.serveHTTP),
		Protocols:         protocols,
		ReadHeaderTimeout: time.Duration(cfg.HTTP1.HeaderReadTimeoutMs) * time.Millisecond,
		// Without it idle keep-alive connections would live, and count
		// against the backpressure limit, forever.
		IdleTimeout:    time.Duration(cfg.KeepAliveTimeout) * time.Second,
		MaxHeaderBytes: cfg.HTTP1.MaxBufferSize,
		ConnContext:    w.connContext,
		ErrorLog:       newServerErrorLog(),
		HTTP2: &http.HTTP2Config{
			MaxConcurrentStreams:          cfg.HTTP2.MaxConcurrentStreams,
			MaxReadFrameSize:              cfg.HTTP2.MaxFrameSize,
			MaxReceiveBufferPerConnection: cfg.HTTP2.InitialConnectionWindowSize,
			MaxReceiveBufferPerStream:     cfg.HTTP2.InitialStreamWindowSize,
			PingTimeout:                   time.Duration(cfg.HTTP2.KeepAliveTimeoutS) * time.Second,
			SendPingTimeout:               time.Duration(cfg.HTTP2.KeepAliveIntervalMs) * time.Millisecond,
		},
	}
	if cfg.HTTP == config.HTTP2 {
		srv.MaxHeaderBytes = cfg.HTTP2.MaxHeadersSize
	}
	srv.SetKeepAlivesEnabled(cfg.HTTP1.KeepAlive)

	if cfg.TLSEnabled() {
		tc, err := tlsx.ServerConfig(cfg)
		if err != nil {
			return nil, err
		}
		srv.TLSConfig = tc
	}
	return srv, nil
}

// serveHTTP is the entry point of every request.
func (w *Worker) serveHTTP(rw http.ResponseWriter, r *http.Request) {
	if w.metrics.ReqHandled.Add(1) == w.maxRequests {
		// served its share: the supervisor starts a replacement, then
		// stops this worker gracefully
		go w.ipc.send(Message{Recycle: true})
	}

	if w.files != nil {
		rawPath, _ := requestTarget(r)
		if file, res := w.files.Match(rawPath); res != static.NoMatch {
			w.metrics.ReqStaticHandled.Add(1)
			if res == static.NotFound {
				w.metrics.ReqStaticErr.Add(1)
				static.NotFoundResponse(rw)
				return
			}
			if !w.files.Serve(rw, file) {
				logx.Infof("Request static file %s not found", file)
			}
			return
		}
	}

	ci := connInfoOf(r)
	switch w.cfg.Interface {
	case config.InterfaceRSGI:
		if w.cfg.Websocket && isWebsocketUpgrade(r) {
			w.serveRSGIWebsocket(rw, r, ci)
		} else {
			w.serveRSGI(rw, r, ci)
		}
	case config.InterfaceWSGI:
		w.serveWSGI(rw, r, ci)
	default:
		if w.cfg.Websocket && isWebsocketUpgrade(r) {
			w.serveASGIWebsocket(rw, r, ci)
		} else {
			w.serveASGI(rw, r, ci)
		}
	}
}

// dispatch hands a request (or websocket) to the async application: state
// becomes the Go side of the protocol object, built on the loop thread.
func (w *Worker) dispatch(rsgi bool, kind C.int, state any, r *http.Request, ci *connInfo) {
	h := C.uintptr_t(handles.put(state))
	head := buildHead(r, ci, kind == C.GC_KIND_WS)
	w.loop.Submit(func() {
		hp, hn := cbuf(*head)
		if rsgi {
			C.balius_rsgi_dispatch(kind, h, hp, hn)
		} else {
			C.balius_asgi_dispatch(kind, h, hp, hn)
		}
		putHead(head)
	})
}

// shutdown gracefully stops the worker: no new connections, in-flight
// requests run to completion, then the interpreter is told to wind down.
func (w *Worker) shutdown() {
	w.shutdownOnce.Do(func() {
		go func() {
			logx.Infof("Stopping worker-%d", w.id)
			w.ws.closeAll()
			if w.srv != nil {
				_ = w.srv.Shutdown(context.Background())
			}
			w.ws.wait(5 * time.Second)
			if w.loop != nil {
				// Runs on the loop thread, after everything queued so far.
				w.loop.Submit(func() { C.balius_call_stop() })
			}
			close(w.stopped)
		}()
	})
}

func (w *Worker) watchSignals() {
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	w.shutdown()
}

// watchSupervisor stops the worker when the supervisor goes away.
func (w *Worker) watchSupervisor() {
	buf := make([]byte, 64)
	for {
		if _, err := w.control.Read(buf); err != nil {
			break
		}
	}
	w.shutdown()
}

type connStats struct{ m *metrics.Worker }

func (s connStats) ConnAccepted() {
	s.m.ConnHandled.Add(1)
	s.m.ConnActive.Add(1)
}
func (s connStats) ConnClosed() { s.m.ConnActive.Add(-1) }
func (s connStats) ConnError()  { s.m.ConnErr.Add(1) }

// serverErrorLog routes net/http's internal error messages to our logger.
type serverErrorLog struct{}

func newServerErrorLog() *log.Logger { return log.New(serverErrorLog{}, "", 0) }

func (serverErrorLog) Write(p []byte) (int, error) {
	msg := strings.TrimSpace(string(p))
	// connection level noise (resets, bad handshakes) is debug material
	logx.Debugf("%s", msg)
	return len(p), nil
}
