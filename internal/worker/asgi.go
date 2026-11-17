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

	"github.com/mirzomumin/balius/internal/logx"
)

// respKind tells the handler goroutine how to produce the response.
type respKind uint8

const (
	respFull respKind = iota
	respStream
	respFile
	respFileRange
	respFailure
)

// response is what the application side hands to the goroutine owning the
// http.ResponseWriter.
type response struct {
	kind       respKind
	status     int
	headers    []headerKV
	body       []byte
	path       string
	start, end int64
	hold       pyObj // a Python object body points into, released once written
}

// asgiHTTP is the state of one ASGI HTTP request.
//
// Two parties share it: the handler goroutine (the only one allowed to
// touch the ResponseWriter) and the event loop thread running the
// application, which drives it through `receive` and `send`.
type asgiHTTP struct {
	w    *Worker
	r    *http.Request
	body reqBody

	// response flow; fields below mu are only touched by the loop thread
	// except where noted
	mu       sync.Mutex
	started  bool      // http.response.start received
	chunked  bool      // the response is being streamed
	finished bool      // the application completed the response
	intent   *response // start message waiting for the first body
	respCh   chan response
	bodyCh   chan chunk
	sentCode atomic.Int32

	txDone *onceSignal // the application sent the whole response
	gone   *onceSignal // the request is over (finished or client gone)
	abort  *onceSignal // the application returned mid-stream

	// request body
	rxExhausted atomic.Bool // the final http.request event was produced
	rxClosed    atomic.Bool // the disconnection was observed
	rxRead      int64       // body bytes read so far (by readBody only)
	over        bool        // the handler returned; under mu
}

func (w *Worker) serveASGI(rw http.ResponseWriter, r *http.Request, ci *connInfo) {
	p := &asgiHTTP{
		w:      w,
		r:      r,
		respCh: make(chan response, 1),
		bodyCh: make(chan chunk, 4),
		txDone: newOnceSignal(),
		gone:   newOnceSignal(),
		abort:  newOnceSignal(),
	}
	p.body.init(rw, r)
	p.sentCode.Store(500)
	w.dispatch(false, C.GC_KIND_HTTP, p, r, ci)

	defer p.finish()
	select {
	case resp := <-p.respCh:
		p.write(rw, resp)
	case <-r.Context().Done():
		// the client went away before the application answered
	}
}

// finish runs when the handler goroutine is about to return.
func (p *asgiHTTP) finish() {
	p.gone.set()
	p.body.finish()
	p.mu.Lock()
	p.over = true
	select {
	case resp := <-p.respCh:
		// never written: the client left first
		p.w.release(resp.hold)
	default:
	}
	p.mu.Unlock()
}

func (p *asgiHTTP) write(rw http.ResponseWriter, resp response) {
	switch resp.kind {
	case respFull:
		writeFull(rw, resp.status, resp.headers, resp.body)
		p.w.release(resp.hold)
	case respFile:
		writeFile(rw, resp.status, resp.headers, resp.path, 0, 0)
	case respStream:
		streamResponse(rw, p.body.rc, p.r, resp, p.bodyCh, p.abort)
	default:
		write500(rw)
	}
}

// streamResponse writes a chunked response, flushing whenever the
// application has nothing more queued.
func streamResponse(rw http.ResponseWriter, rc *http.ResponseController, r *http.Request, resp response,
	chunks <-chan chunk, abort *onceSignal,
) {
	applyHeaders(rw.Header(), resp.headers)
	rw.WriteHeader(resp.status)

	done := r.Context().Done()
	for {
		var c chunk
		select {
		case c = <-chunks:
		default:
			// nothing ready: push out what we have, then wait
			if rc.Flush() != nil {
				return
			}
			select {
			case c = <-chunks:
			case <-done:
				return
			case <-abort.ch:
				// The application returned without completing the body:
				// tear the connection down so the client can tell the
				// response is truncated.
				panic(http.ErrAbortHandler)
			}
		}
		if len(c.data) > 0 {
			if _, err := rw.Write(c.data); err != nil {
				return
			}
		}
		if c.last {
			return
		}
	}
}

// appDone implements appWatcher: the application callable returned.
func (p *asgiHTTP) appDone() {
	p.mu.Lock()
	switch {
	case p.chunked:
		if !p.finished {
			p.abort.set()
		}
	case !p.finished:
		// no (complete) response at all
		select {
		case p.respCh <- response{kind: respFailure}:
		default:
		}
	}
	p.finished = true
	p.mu.Unlock()
	p.txDone.set()
}

// ---- receive ---------------------------------------------------------------

func (p *asgiHTTP) receive() pyObj {
	if p.rxClosed.Load() {
		return C.balius_done(C.balius_asgi_msg_disconnect())
	}
	exhausted := p.rxExhausted.Load()
	if !exhausted && p.body.empty {
		// nothing to wait for: answer without suspending the coroutine
		p.rxExhausted.Store(true)
		return C.balius_done(C.balius_asgi_msg_request(nil, 0, 0))
	}
	// a body of known length is read straight into the bytes object the
	// application gets
	var (
		body pyObj
		dst  []byte
	)
	if left := p.r.ContentLength - p.rxRead; !exhausted && p.r.ContentLength > 0 && left > 0 {
		if body, dst = pyBytesAlloc(int(min(left, asgiEventMax))); body == nil {
			return nil
		}
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		C.balius_decref(body)
		return nil
	}
	switch {
	case exhausted:
		// the body is consumed: the next event is the disconnection
		go func() {
			select {
			case <-p.txDone.ch:
			case <-p.gone.ch:
				p.rxClosed.Store(true)
			}
			fut.resolveWith(func() pyObj { return C.balius_asgi_msg_disconnect() })
		}()
	case body != nil:
		go p.readInto(fut, body, dst)
	default:
		go p.readBody(fut)
	}
	return obj
}

// The largest `http.request` event made of a body of known length.
const asgiEventMax = 1 << 20

// readInto produces the next `http.request` event of a body of known
// length: up to asgiEventMax bytes at once, as every event is a round trip
// through the event loop and applications typically join the pieces back
// together. dst is the contents of body, which nothing else references.
func (p *asgiHTTP) readInto(fut pyFuture, body pyObj, dst []byte) {
	n, err := p.body.readFull(dst)
	if p.rxRead += int64(n); err == nil && p.rxRead == p.r.ContentLength {
		err = io.EOF
	}
	if err != nil && !errors.Is(err, io.EOF) {
		// the client went away (or the request is already over)
		p.rxExhausted.Store(true)
		p.txDone.set()
		fut.resolveWith(func() pyObj {
			C.balius_decref(body)
			return C.balius_asgi_msg_disconnect()
		})
		return
	}
	more := C.int(1)
	if err != nil {
		more = 0
		p.rxExhausted.Store(true)
	}
	fut.resolveWith(func() pyObj {
		return C.balius_asgi_msg_request_obj(C.balius_bytes_shrink(body, C.Py_ssize_t(n)), more)
	})
}

// readBody produces the next `http.request` event of a body of unknown
// length, as it arrives.
func (p *asgiHTTP) readBody(fut pyFuture) {
	bp := bodyBufPool.Get().(*[]byte)
	buf := *bp
	n, err := p.body.read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		// the client went away (or the request is already over)
		bodyBufPool.Put(bp)
		p.rxExhausted.Store(true)
		p.txDone.set()
		fut.resolveWith(func() pyObj { return C.balius_asgi_msg_disconnect() })
		return
	}
	more := err == nil
	if !more {
		p.rxExhausted.Store(true)
	}
	fut.resolveWith(func() pyObj {
		cp, cn := cbuf(buf[:n])
		var cmore C.int
		if more {
			cmore = 1
		}
		msg := C.balius_asgi_msg_request(cp, cn, cmore)
		bodyBufPool.Put(bp)
		return msg
	})
}

// ---- send ------------------------------------------------------------------

func (p *asgiHTTP) sendStart(status int, headers []headerKV) pyObj {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return pyRaise(C.GC_ERR_RUNTIME, "ASGI flow error: Response already started")
	}
	p.started = true
	// Deferring the response until the first body message lets the common
	// "start + single body" case go out as one fixed-length response;
	// event streams have to start flowing right away though.
	if !headerHasPrefix(headers, "content-type", "text/event-stream") {
		p.intent = &response{status: status, headers: headers}
		return pyEmpty()
	}
	p.chunked = true
	p.dispatch(response{kind: respStream, status: status, headers: headers})
	return pyEmpty()
}

// dispatch hands the response to the handler goroutine. Called with mu held,
// on the loop thread.
func (p *asgiHTTP) dispatch(resp response) {
	p.sentCode.Store(int32(resp.status))
	if !p.over {
		select {
		case p.respCh <- resp:
			return
		default:
		}
	}
	if resp.hold != nil {
		C.balius_decref(resp.hold)
	}
}

// sendBody handles `http.response.body`. src is the body's memory, owned by
// Python; obj, when not nil, is an immutable object src points into, which
// a complete response holds on to instead of copying src.
func (p *asgiHTTP) sendBody(obj pyObj, src []byte, more bool) pyObj {
	p.mu.Lock()
	switch {
	case !p.started:
		p.mu.Unlock()
		return pyRaise(C.GC_ERR_RUNTIME, "ASGI flow error: Response not started")

	case !p.chunked && !more:
		intent := p.intent
		if intent == nil {
			p.mu.Unlock()
			return pyRaise(C.GC_ERR_RUNTIME, "ASGI flow error: Response already finished")
		}
		p.intent = nil
		p.finished = true
		resp := response{kind: respFull, status: intent.status, headers: intent.headers}
		if obj != nil && len(src) >= holdThreshold {
			C.balius_incref(obj)
			resp.body, resp.hold = src, obj
		} else {
			resp.body = bytes.Clone(src)
		}
		p.dispatch(resp)
		p.mu.Unlock()
		p.txDone.set()
		return pyEmpty()

	case !p.chunked:
		body := bytes.Clone(src)
		intent := p.intent
		if intent == nil {
			p.mu.Unlock()
			return pyRaise(C.GC_ERR_RUNTIME, "ASGI flow error: Response already finished")
		}
		p.intent = nil
		p.chunked = true
		p.dispatch(response{kind: respStream, status: intent.status, headers: intent.headers})
		p.mu.Unlock()
		return p.pushChunk(chunk{data: body})

	case p.finished:
		p.mu.Unlock()
		return pyRaise(C.GC_ERR_RUNTIME, "ASGI flow error: Transport not initialized or closed")

	default:
		if !more {
			p.finished = true
		}
		p.mu.Unlock()
		return p.pushChunk(chunk{data: bytes.Clone(src), last: !more})
	}
}

// pushChunk queues a body chunk; when the queue is full the returned
// awaitable completes once there is room again, which is how backpressure
// reaches the application.
func (p *asgiHTTP) pushChunk(c chunk) pyObj {
	select {
	case p.bodyCh <- c:
		if c.last {
			p.txDone.set()
		}
		return pyEmpty()
	default:
	}
	if p.gone.isSet() {
		p.transportError()
		return pyEmpty()
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	go func() {
		select {
		case p.bodyCh <- c:
			if c.last {
				p.txDone.set()
			}
		case <-p.gone.ch:
			p.transportError()
		}
		fut.resolveNone()
	}()
	return obj
}

func (p *asgiHTTP) transportError() {
	if !p.rxClosed.Load() {
		logx.Infof("ASGI transport error: the client closed the connection")
	}
	p.txDone.set()
}

func (p *asgiHTTP) sendFile(path string) pyObj {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started || p.intent == nil {
		return pyRaise(C.GC_ERR_RUNTIME, "ASGI flow error: Response not started")
	}
	intent := p.intent
	p.intent = nil
	p.finished = true
	p.dispatch(response{kind: respFile, status: intent.status, headers: intent.headers, path: path})
	p.txDone.set()
	return pyEmpty()
}

// ---- exports ---------------------------------------------------------------

// asgiProto is what both the HTTP and the websocket state implement.
type asgiProto interface {
	receive() pyObj
}

func unsupportedASGIMessage() pyObj {
	return pyRaise(C.GC_ERR_RUNTIME, "Unsupported ASGI message")
}

//export goAsgiReceive
func goAsgiReceive(h C.uintptr_t) pyObj {
	if p, ok := handles.get(uintptr(h)).(asgiProto); ok {
		return p.receive()
	}
	return unsupportedASGIMessage()
}

//export goAsgiSendStart
func goAsgiSendStart(h C.uintptr_t, ws C.int, status C.int, hdr *C.char, hlen C.Py_ssize_t, nh C.int) pyObj {
	headers := parseHeaders(unsafe.Pointer(hdr), int(hlen), int(nh))
	switch p := handles.get(uintptr(h)).(type) {
	case *asgiHTTP:
		if ws == 0 {
			return p.sendStart(int(status), headers)
		}
	case *asgiWS:
		if ws != 0 {
			return p.rejectStart(int(status), headers)
		}
	}
	return unsupportedASGIMessage()
}

//export goAsgiSendBody
func goAsgiSendBody(h C.uintptr_t, ws C.int, obj pyObj, body *C.char, blen C.Py_ssize_t, more C.int) pyObj {
	switch p := handles.get(uintptr(h)).(type) {
	case *asgiHTTP:
		if ws == 0 {
			var src []byte
			if blen > 0 {
				src = unsafe.Slice((*byte)(unsafe.Pointer(body)), int(blen))
			}
			return p.sendBody(obj, src, more != 0)
		}
	case *asgiWS:
		if ws != 0 {
			return p.rejectBody(C.GoBytes(unsafe.Pointer(body), C.int(blen)), more != 0)
		}
	}
	return unsupportedASGIMessage()
}

//export goAsgiSendFile
func goAsgiSendFile(h C.uintptr_t, path *C.char, plen C.Py_ssize_t) pyObj {
	if p, ok := handles.get(uintptr(h)).(*asgiHTTP); ok {
		return p.sendFile(C.GoStringN(path, C.int(plen)))
	}
	return unsupportedASGIMessage()
}

//export goAsgiSentCode
func goAsgiSentCode(h C.uintptr_t) C.int {
	if p, ok := handles.get(uintptr(h)).(*asgiHTTP); ok {
		return C.int(p.sentCode.Load())
	}
	return 500
}
