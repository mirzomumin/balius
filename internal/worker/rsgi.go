package worker

/*
#include "glue.h"
*/
import "C"

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	rsgiProtocolError = "RSGI protocol error"
	rsgiClosedError   = "RSGI transport is closed"
)

// rsgiHTTP is the state of one RSGI HTTP request.
type rsgiHTTP struct {
	w    *Worker
	r    *http.Request
	body reqBody

	mu        sync.Mutex
	responded bool
	over      bool // the handler returned
	respCh    chan response
	stream    *mailbox[[]byte] // body of a streamed response

	gone *onceSignal // the request is over (finished or client gone)

	// request body; touched by the loop thread only unless atomic
	bodyTaken bool
	iterating bool
	iterDone  atomic.Bool
}

func (w *Worker) serveRSGI(rw http.ResponseWriter, r *http.Request, ci *connInfo) {
	p := &rsgiHTTP{
		w:      w,
		r:      r,
		respCh: make(chan response, 1),
		gone:   newOnceSignal(),
	}
	p.body.init(rw, r)
	w.dispatch(true, C.GC_KIND_HTTP, p, r, ci)

	defer func() {
		p.gone.set()
		p.body.finish()
		p.mu.Lock()
		p.over = true
		select {
		case resp := <-p.respCh:
			// never written: the client left first
			w.release(resp.hold)
		default:
		}
		p.mu.Unlock()
	}()
	select {
	case resp := <-p.respCh:
		p.write(rw, resp)
	case <-r.Context().Done():
	}
}

func (p *rsgiHTTP) write(rw http.ResponseWriter, resp response) {
	switch resp.kind {
	case respFull:
		writeFull(rw, resp.status, resp.headers, resp.body)
		p.w.release(resp.hold)
	case respFile:
		writeFile(rw, resp.status, resp.headers, resp.path, 0, 0)
	case respFileRange:
		writeFile(rw, resp.status, resp.headers, resp.path, resp.start, resp.end)
	case respStream:
		p.writeStream(rw, resp)
	default:
		writeError(rw, http.StatusInternalServerError, "")
	}
}

// writeStream sends the chunks the application pushes on its transport
// until the transport is dropped.
func (p *rsgiHTTP) writeStream(rw http.ResponseWriter, resp response) {
	applyHeaders(rw.Header(), resp.headers)
	rw.WriteHeader(resp.status)

	done := p.r.Context().Done()
	var batch [][]byte
	for {
		var closed bool
		batch, closed = p.stream.take(batch)
		for i, data := range batch {
			if len(data) > 0 {
				if _, err := rw.Write(data); err != nil {
					return
				}
			}
			batch[i] = nil
		}
		if closed {
			return
		}
		if p.body.rc.Flush() != nil {
			return
		}
		select {
		case <-p.stream.notify:
		case <-done:
			return
		}
	}
}

// appDone implements appWatcher.
func (p *rsgiHTTP) appDone() {
	p.mu.Lock()
	if !p.responded {
		p.responded = true
		p.respCh <- response{kind: respFailure}
	}
	p.mu.Unlock()
}

// ---- request body ----------------------------------------------------------

// readAll implements `await protocol()`.
func (p *rsgiHTTP) readAll() pyObj {
	if p.bodyTaken {
		return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
	}
	p.bodyTaken = true
	if p.body.empty {
		return C.balius_done(C.balius_bytes(nil, 0))
	}
	if n := p.r.ContentLength; n > 0 && n <= rsgiDirectMax {
		return p.readAllInto(int(n))
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	go func() {
		var data bytes.Buffer
		if n := p.r.ContentLength; n > 0 && n <= 64<<20 {
			data.Grow(int(n))
		}
		bp := bodyBufPool.Get().(*[]byte)
		defer bodyBufPool.Put(bp)
		for {
			n, err := p.body.read(*bp)
			data.Write((*bp)[:n])
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				fut.fail(C.GC_ERR_RSGI_CLOSED, rsgiClosedError)
				return
			}
		}
		fut.resolveWith(func() pyObj { return pyBytes(data.Bytes()) })
	}()
	return obj
}

// Bodies of known length up to this size are read straight into the bytes
// object `await protocol()` returns.
const rsgiDirectMax = 64 << 20

func (p *rsgiHTTP) readAllInto(size int) pyObj {
	body, dst := pyBytesAlloc(size)
	if body == nil {
		return nil
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		C.balius_decref(body)
		return nil
	}
	go func() {
		n, err := p.body.readFull(dst)
		if err != nil && !errors.Is(err, io.EOF) {
			fut.resolveWith(func() pyObj {
				C.balius_decref(body)
				return pyRaise(C.GC_ERR_RSGI_CLOSED, rsgiClosedError)
			})
			return
		}
		fut.resolveWith(func() pyObj { return C.balius_bytes_shrink(body, C.Py_ssize_t(n)) })
	}()
	return obj
}

// readChunk implements `__anext__`.
func (p *rsgiHTTP) readChunk() pyObj {
	if !p.iterating || p.iterDone.Load() || p.body.empty {
		return pyRaise(C.GC_ERR_STOP_ASYNC_ITER, "stream exhausted")
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	go func() {
		bp := bodyBufPool.Get().(*[]byte)
		n, err := p.body.read(*bp)
		if err != nil {
			p.iterDone.Store(true)
		}
		switch {
		case err != nil && !errors.Is(err, io.EOF):
			// a truncated body must not look complete
			bodyBufPool.Put(bp)
			fut.fail(C.GC_ERR_RSGI_CLOSED, rsgiClosedError)
			return
		case n == 0:
			bodyBufPool.Put(bp)
			fut.fail(C.GC_ERR_STOP_ASYNC_ITER, "stream exhausted")
			return
		}
		fut.resolveWith(func() pyObj {
			chunk := pyBytes((*bp)[:n])
			bodyBufPool.Put(bp)
			return chunk
		})
	}()
	return obj
}

func (p *rsgiHTTP) clientDisconnect() pyObj {
	if p.gone.isSet() {
		return pyEmpty()
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	go func() {
		<-p.gone.ch
		fut.resolveNone()
	}()
	return obj
}

// ---- exports ---------------------------------------------------------------

//export goRsgiBody
func goRsgiBody(h C.uintptr_t) pyObj {
	if p, ok := handles.get(uintptr(h)).(*rsgiHTTP); ok {
		return p.readAll()
	}
	return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
}

//export goRsgiAiter
func goRsgiAiter(h C.uintptr_t) C.int {
	p, ok := handles.get(uintptr(h)).(*rsgiHTTP)
	if !ok || p.bodyTaken {
		return 1
	}
	p.bodyTaken = true
	p.iterating = true
	return 0
}

//export goRsgiAnext
func goRsgiAnext(h C.uintptr_t) pyObj {
	if p, ok := handles.get(uintptr(h)).(*rsgiHTTP); ok {
		return p.readChunk()
	}
	return pyRaise(C.GC_ERR_STOP_ASYNC_ITER, "stream exhausted")
}

//export goRsgiClientDisconnect
func goRsgiClientDisconnect(h C.uintptr_t) pyObj {
	if p, ok := handles.get(uintptr(h)).(*rsgiHTTP); ok {
		return p.clientDisconnect()
	}
	return pyEmpty()
}

// goRsgiResponse returns 0 when the response was taken, 1 when the request
// was already answered.
//
//export goRsgiResponse
func goRsgiResponse(h C.uintptr_t, kind C.int, status C.int, hdr *C.char, hlen C.Py_ssize_t, nh C.int,
	hold pyObj, body *C.char, blen C.Py_ssize_t, start C.ulonglong, end C.ulonglong,
) C.int {
	p, ok := handles.get(uintptr(h)).(*rsgiHTTP)
	if !ok {
		return 1
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.responded {
		return 1
	}
	p.responded = true

	resp := response{
		status:  int(status),
		headers: parseHeaders(unsafe.Pointer(hdr), int(hlen), int(nh)),
	}
	switch kind {
	case C.RK_EMPTY:
		resp.kind = respFull
	case C.RK_BYTES, C.RK_STR:
		resp.kind = respFull
		if hold != nil && blen >= holdThreshold {
			// immutable: written from where it is, instead of copied
			C.balius_incref(hold)
			resp.body = unsafe.Slice((*byte)(unsafe.Pointer(body)), int(blen))
			resp.hold = hold
		} else {
			resp.body = C.GoBytes(unsafe.Pointer(body), C.int(blen))
		}
	case C.RK_FILE:
		resp.kind = respFile
		resp.path = C.GoStringN(body, C.int(blen))
	case C.RK_FILE_RANGE:
		resp.kind = respFileRange
		resp.path = C.GoStringN(body, C.int(blen))
		resp.start, resp.end = int64(start), int64(end)
	case C.RK_STREAM:
		resp.kind = respStream
		p.stream = newMailbox[[]byte]()
	}
	if p.over {
		// the client left: nobody will write it
		C.balius_decref(resp.hold)
		return 0
	}
	p.respCh <- resp
	return 0
}

//export goRsgiStreamSend
func goRsgiStreamSend(h C.uintptr_t, data *C.char, dlen C.Py_ssize_t) pyObj {
	p, ok := handles.get(uintptr(h)).(*rsgiHTTP)
	if !ok || p.stream == nil || p.gone.isSet() ||
		!p.stream.push(C.GoBytes(unsafe.Pointer(data), C.int(dlen))) {
		return pyRaise(C.GC_ERR_RSGI_CLOSED, rsgiClosedError)
	}
	return pyEmpty()
}

//export goRsgiStreamClose
func goRsgiStreamClose(h C.uintptr_t) {
	if p, ok := handles.get(uintptr(h)).(*rsgiHTTP); ok && p.stream != nil {
		p.stream.close()
	}
}
