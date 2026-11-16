package worker

/*
#include <stdlib.h>
#include "glue.h"
*/
import "C"

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/mirzomumin/balius/internal/config"
	"github.com/mirzomumin/balius/internal/logx"
)

// The Python half of balius: the modules below are compiled into the
// binary and served to the interpreter by a meta path finder.
//
//go:embed all:pysrc
var pySources embed.FS

// pyObj is a reference to a Python object. It must only be touched while
// holding the GIL.
type pyObj = *C.PyObject

// pythonExecutable picks the interpreter path the embedded runtime should
// impersonate, which is what selects the virtualenv (if any).
func pythonExecutable(cfg *config.Config) string {
	if cfg.Python != "" {
		if p, err := exec.LookPath(cfg.Python); err == nil {
			if abs, err := filepath.Abs(p); err == nil {
				return abs
			}
			return p
		}
		return cfg.Python
	}
	if venv := os.Getenv("VIRTUAL_ENV"); venv != "" {
		for _, name := range []string{"python3", "python"} {
			p := filepath.Join(venv, "bin", name)
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}
	return ""
}

// initPython boots the interpreter on the calling thread (which must be
// the process main thread) and installs the embedded modules.
func initPython(cfg *config.Config) error {
	var cexe *C.char
	if exe := pythonExecutable(cfg); exe != "" {
		cexe = C.CString(exe)
		defer C.free(unsafe.Pointer(cexe))
	}
	errbuf := (*C.char)(C.calloc(1, 512))
	defer C.free(unsafe.Pointer(errbuf))
	if C.balius_py_init(cexe, errbuf, 512) != 0 {
		return fmt.Errorf("unable to initialize Python: %s", C.GoString(errbuf))
	}

	var bootstrap string
	err := fs.WalkDir(pySources, "pysrc", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, ".py") {
			return err
		}
		src, err := pySources.ReadFile(p)
		if err != nil {
			return err
		}
		rel := strings.TrimSuffix(strings.TrimPrefix(p, "pysrc/"), ".py")
		if rel == "_bootstrap" {
			bootstrap = string(src)
			return nil
		}
		isPkg := 0
		if path.Base(rel) == "__init__" {
			rel = path.Dir(rel)
			isPkg = 1
		}
		name := C.CString(strings.ReplaceAll(rel, "/", "."))
		defer C.free(unsafe.Pointer(name))
		csrc := C.CString(string(src))
		defer C.free(unsafe.Pointer(csrc))
		if C.balius_py_add_source(name, csrc, C.Py_ssize_t(len(src)), C.int(isPkg)) != 0 {
			return fmt.Errorf("unable to register embedded module %s", rel)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if bootstrap == "" {
		return errors.New("embedded bootstrap module is missing")
	}
	cboot := C.CString(bootstrap)
	defer C.free(unsafe.Pointer(cboot))
	if C.balius_py_bootstrap(cboot) != 0 {
		return errors.New("unable to install the embedded modules importer")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Event loop bridge
// ---------------------------------------------------------------------------

// loopBridge carries work from goroutines onto the thread running the
// asyncio event loop.
//
// Goroutines queue closures with Submit; the loop watches the read end of a
// pipe (`loop.add_reader`) and, when it becomes readable, calls back into
// goLoopDrain which runs everything queued so far. All the Python work of
// the async interfaces therefore happens on the loop thread: no other
// thread ever competes for the GIL, and any number of events queued between
// two loop iterations cost a single wake-up.
type loopBridge struct {
	mu        sync.Mutex
	queue     []func()
	spare     []func()
	scheduled atomic.Bool
	rfd, wfd  int

	pending atomic.Int64 // queued tasks, for metrics
	busy    atomic.Int64 // cumulative drain time, microseconds
}

func newLoopBridge() (*loopBridge, error) {
	var fds [2]int
	if err := syscall.Pipe(fds[:]); err != nil {
		return nil, fmt.Errorf("pipe: %w", err)
	}
	for _, fd := range fds {
		syscall.CloseOnExec(fd)
		if err := syscall.SetNonblock(fd, true); err != nil {
			return nil, fmt.Errorf("pipe: %w", err)
		}
	}
	return &loopBridge{rfd: fds[0], wfd: fds[1]}, nil
}

var wakeByte = []byte{1}

// Submit queues fn to run on the loop thread with the GIL held.
func (l *loopBridge) Submit(fn func()) {
	l.mu.Lock()
	l.queue = append(l.queue, fn)
	l.mu.Unlock()
	l.pending.Add(1)
	if l.scheduled.CompareAndSwap(false, true) {
		for {
			_, err := syscall.Write(l.wfd, wakeByte)
			if err != syscall.EINTR {
				break
			}
		}
	}
}

// drain runs on the loop thread.
func (l *loopBridge) drain() {
	var buf [64]byte
	for {
		n, err := syscall.Read(l.rfd, buf[:])
		if err == syscall.EINTR {
			continue
		}
		if n < len(buf) {
			break
		}
	}
	// Must be cleared before the queue is swapped: a Submit racing with us
	// either lands in this batch or writes a fresh wake-up byte.
	l.scheduled.Store(false)

	l.mu.Lock()
	tasks := l.queue
	l.queue = l.spare[:0]
	l.mu.Unlock()
	if len(tasks) == 0 {
		l.spare = tasks
		return
	}

	start := time.Now()
	for i, fn := range tasks {
		fn()
		tasks[i] = nil
	}
	l.pending.Add(-int64(len(tasks)))
	l.busy.Add(int64(time.Since(start) / time.Microsecond))
	l.spare = tasks
}

// ---------------------------------------------------------------------------
// Small C API helpers (GIL required)
// ---------------------------------------------------------------------------

func cbuf(b []byte) (*C.char, C.Py_ssize_t) {
	if len(b) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(&b[0])), C.Py_ssize_t(len(b))
}

func cstr(s string) (*C.char, C.Py_ssize_t) {
	if len(s) == 0 {
		return nil, 0
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s))), C.Py_ssize_t(len(s))
}

func pyBytes(b []byte) pyObj {
	p, n := cbuf(b)
	return C.balius_bytes(p, n)
}

// Static error messages, converted once.
var cMessages sync.Map // string -> *C.char

func cmsg(msg string) *C.char {
	if v, ok := cMessages.Load(msg); ok {
		return v.(*C.char)
	}
	c := C.CString(msg)
	if prev, loaded := cMessages.LoadOrStore(msg, c); loaded {
		C.free(unsafe.Pointer(c))
		return prev.(*C.char)
	}
	return c
}

// pyRaise sets a Python exception and returns NULL, ready to be returned
// from an exported function.
func pyRaise(kind C.int, msg string) pyObj {
	return C.balius_raise(kind, cmsg(msg))
}

func pyEmpty() pyObj { return C.balius_empty() }

// pyFuture is an asyncio future owned by the Go side until it is resolved.
// Resolution always happens on the loop thread.
type pyFuture struct {
	obj  pyObj
	loop *loopBridge
}

// newFuture creates a pending future. The returned object reference is the
// one to hand back to Python; ok is false when the future could not be
// created, in which case a Python exception is set.
func (l *loopBridge) newFuture() (pyFuture, pyObj, bool) {
	fut := C.balius_future_new()
	if fut == nil {
		return pyFuture{}, nil, false
	}
	return pyFuture{obj: fut, loop: l}, fut, true
}

// resolveWith completes the future with the object built by build, which
// runs on the loop thread.
func (f pyFuture) resolveWith(build func() pyObj) {
	f.loop.Submit(func() { C.balius_future_set(f.obj, build()) })
}

// resolveNone completes the future with None.
func (f pyFuture) resolveNone() {
	f.loop.Submit(func() { C.balius_future_set(f.obj, C.balius_none()) })
}

// fail completes the future with an exception.
func (f pyFuture) fail(kind C.int, msg string) {
	f.loop.Submit(func() { C.balius_future_fail(f.obj, kind, cmsg(msg)) })
}

// ---------------------------------------------------------------------------
// Exports shared by every interface
// ---------------------------------------------------------------------------

// appWatcher is implemented by handle values that want to know when the
// application callable returned.
type appWatcher interface{ appDone() }

//export goLoopDrain
func goLoopDrain() {
	if w := current; w != nil && w.loop != nil {
		w.loop.drain()
	}
}

//export goLoopWakeFd
func goLoopWakeFd() C.int {
	if w := current; w != nil && w.loop != nil {
		return C.int(w.loop.rfd)
	}
	return -1
}

//export goHandleRelease
func goHandleRelease(h C.uintptr_t) {
	handles.del(uintptr(h))
}

//export goWatcherDone
func goWatcherDone(h C.uintptr_t) {
	if v, ok := handles.get(uintptr(h)).(appWatcher); ok {
		v.appDone()
	}
}

//export goServe
func goServe() *C.char {
	if err := current.start(); err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export goServeWSGI
func goServeWSGI() *C.char {
	// Called without the GIL: blocks until the worker is asked to stop.
	if err := current.start(); err != nil {
		return C.CString(err.Error())
	}
	<-current.stopped
	current.pool.stop()
	return nil
}

var cVersion = C.CString(config.Version)

//export goVersion
func goVersion() *C.char { return cVersion }

//export goLog
func goLog(level C.int, msg *C.char, n C.Py_ssize_t) {
	s := C.GoStringN(msg, C.int(n))
	switch {
	case level >= C.int(logx.LevelError):
		logx.Errorf("%s", s)
	case level >= C.int(logx.LevelWarning):
		logx.Warnf("%s", s)
	case level >= C.int(logx.LevelInfo):
		logx.Infof("%s", s)
	default:
		logx.Debugf("%s", s)
	}
}
