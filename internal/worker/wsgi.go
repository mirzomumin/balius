package worker

/*
#include "glue.h"
*/
import "C"

import (
	"bytes"
	"net/http"
	"sync"
	"unsafe"
)

// wsgiReq is the state of one WSGI request.
//
// The application runs on a pool thread; the handler goroutine waits for
// the response it produces and stays inside ServeHTTP until the
// application returned, so that `wsgi.input` remains readable throughout.
type wsgiReq struct {
	w    *Worker
	r    *http.Request
	body reqBody

	mu        sync.Mutex
	responded bool
	respCh    chan response
	chunks    chan chunk
	gone      *onceSignal // the handler is done with the response
	abort     *onceSignal

	// wsgi.input buffer
	inMu sync.Mutex
	buf  []byte
	eof  bool
}

func (w *Worker) serveWSGI(rw http.ResponseWriter, r *http.Request, ci *connInfo) {
	p := &wsgiReq{
		w:      w,
		r:      r,
		respCh: make(chan response, 1),
		chunks: make(chan chunk, 4),
		gone:   newOnceSignal(),
		abort:  newOnceSignal(),
	}
	if hasBody(r) && r.ProtoMajor >= 2 {
		prefetch := newPrefetchBody(r.Body)
		r.Body = prefetch
		defer prefetch.Close()
	}
	p.body.init(rw, r)
	p.eof = p.body.empty

	h := handles.put(p)
	head := buildWSGIHead(r, ci)
	appDone := make(chan struct{})
	accepted := w.pool.run(func(ts *C.PyThreadState) {
		hp, hn := cbuf(*head)
		var waitUs C.int64_t
		failed := C.tulpar_wsgi_run(ts, C.uintptr_t(h), hp, hn, &waitUs) != 0
		w.metrics.PyWait.Add(int64(waitUs))
		handles.del(h)
		putHead(head)
		if failed {
			p.respond(response{kind: respFailure})
		}
		close(appDone)
	})
	if !accepted {
		// the worker is shutting down: the application will never run
		handles.del(h)
		putHead(head)
		writeError(rw, http.StatusServiceUnavailable, "")
		return
	}

	select {
	case resp := <-p.respCh:
		switch resp.kind {
		case respFull:
			writeFull(rw, resp.status, resp.headers, resp.body)
		case respStream:
			streamResponse(rw, p.body.rc, r, resp, p.chunks, p.abort)
		default:
			writeError(rw, http.StatusInternalServerError, "")
		}
	case <-r.Context().Done():
	}
	p.gone.set()
	// The application may still be reading the request: it has to be done
	// before the request body is given back to net/http.
	<-appDone
	p.body.finish()
}

// respond hands the response to the handler goroutine; it reports false
// when the request was already answered.
func (p *wsgiReq) respond(resp response) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.responded {
		return false
	}
	p.responded = true
	p.respCh <- resp
	return true
}

// fill buffers the request body until `mode` is satisfied (or the body
// ends) and returns how many bytes can be taken. Called without the GIL.
func (p *wsgiReq) fill(mode int, size int) int {
	p.inMu.Lock()
	defer p.inMu.Unlock()

	scanned := 0
	ready := func() (int, bool) {
		switch mode {
		case C.BODY_SIZE:
			if len(p.buf) >= size {
				return size, true
			}
		case C.BODY_LINE:
			if i := bytes.IndexByte(p.buf[scanned:], '\n'); i >= 0 {
				return scanned + i + 1, true
			}
			scanned = len(p.buf)
		}
		return len(p.buf), false
	}
	n, ok := ready()
	if ok || p.eof {
		return n
	}
	bp := bodyBufPool.Get().(*[]byte)
	defer bodyBufPool.Put(bp)
	for {
		rn, err := p.body.read(*bp)
		p.buf = append(p.buf, (*bp)[:rn]...)
		if err != nil {
			p.eof = true
		}
		if n, ok = ready(); ok || p.eof {
			return n
		}
	}
}

// take moves n buffered bytes into dst.
func (p *wsgiReq) take(dst []byte) {
	p.inMu.Lock()
	n := copy(dst, p.buf)
	p.buf = p.buf[n:]
	if len(p.buf) == 0 {
		p.buf = nil
	}
	p.inMu.Unlock()
}

// ---- exports ---------------------------------------------------------------

//export goWsgiResponse
func goWsgiResponse(h C.uintptr_t, status C.int, hdr *C.char, hlen C.Py_ssize_t, nh C.int,
	body *C.char, blen C.Py_ssize_t,
) {
	if p, ok := handles.get(uintptr(h)).(*wsgiReq); ok {
		p.respond(response{
			kind:    respFull,
			status:  int(status),
			headers: parseHeaders(unsafe.Pointer(hdr), int(hlen), int(nh)),
			body:    C.GoBytes(unsafe.Pointer(body), C.int(blen)),
		})
	}
}

// goWsgiIterStart returns 0 when the streamed response was started.
//
//export goWsgiIterStart
func goWsgiIterStart(h C.uintptr_t, status C.int, hdr *C.char, hlen C.Py_ssize_t, nh C.int) C.int {
	p, ok := handles.get(uintptr(h)).(*wsgiReq)
	if !ok {
		return 1
	}
	started := p.respond(response{
		kind:    respStream,
		status:  int(status),
		headers: parseHeaders(unsafe.Pointer(hdr), int(hlen), int(nh)),
	})
	if !started {
		return 1
	}
	return 0
}

// goWsgiChunk queues a body chunk, waiting for room (without the GIL). It
// returns non-zero when nobody is listening anymore.
//
//export goWsgiChunk
func goWsgiChunk(h C.uintptr_t, data *C.char, dlen C.Py_ssize_t) C.int {
	p, ok := handles.get(uintptr(h)).(*wsgiReq)
	if !ok {
		return 1
	}
	c := chunk{data: C.GoBytes(unsafe.Pointer(data), C.int(dlen))}
	select {
	case p.chunks <- c:
		return 0
	case <-p.gone.ch:
		return 1
	}
}

//export goWsgiIterEnd
func goWsgiIterEnd(h C.uintptr_t, failed C.int) {
	if p, ok := handles.get(uintptr(h)).(*wsgiReq); ok {
		if failed != 0 {
			p.abort.set() // truncated: the handler drops the connection
			return
		}
		select {
		case p.chunks <- chunk{last: true}:
		case <-p.gone.ch:
		}
	}
}

//export goWsgiBodyFill
func goWsgiBodyFill(h C.uintptr_t, mode C.int, size C.Py_ssize_t) C.Py_ssize_t {
	if p, ok := handles.get(uintptr(h)).(*wsgiReq); ok {
		return C.Py_ssize_t(p.fill(int(mode), int(size)))
	}
	return 0
}

//export goWsgiBodyTake
func goWsgiBodyTake(h C.uintptr_t, dst *C.char, n C.Py_ssize_t) {
	if p, ok := handles.get(uintptr(h)).(*wsgiReq); ok && n > 0 {
		p.take(unsafe.Slice((*byte)(unsafe.Pointer(dst)), int(n)))
	}
}
