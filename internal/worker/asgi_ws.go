package worker

/*
#include "glue.h"
*/
import "C"

import (
	"net/http"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/mirzomumin/balius/internal/logx"
)

// wsDecision is the application's verdict on a websocket handshake.
type wsDecision struct {
	accept      bool
	subprotocol string
	headers     []headerKV
	status      int
	body        []byte
	fut         pyFuture // accept only
	self        pyObj    // RSGI accept only: the protocol object
}

// asgiWS is the state of one ASGI websocket connection.
type asgiWS struct {
	w *Worker

	mu       sync.Mutex
	decided  bool      // the handshake was accepted or rejected
	gone     bool      // the client left before a decision
	intent   *response // websocket.http.response.start waiting for its body
	conn     *wsConn   // set once the connection is upgraded
	decision chan wsDecision

	accepted *onceSignal // the handshake is settled, one way or the other
	closed   atomic.Bool
	initRx   bool // the first receive() already happened (loop thread only)
}

func (w *Worker) serveASGIWebsocket(rw http.ResponseWriter, r *http.Request, ci *connInfo) {
	if reason := checkWebsocketHandshake(r); reason != "" {
		logx.Infof("Websocket handshake failed with %s", reason)
		writeError(rw, http.StatusBadRequest, reason)
		return
	}
	p := &asgiWS{
		w:        w,
		decision: make(chan wsDecision, 1),
		accepted: newOnceSignal(),
	}
	w.dispatch(false, C.GC_KIND_WS, p, r, ci)

	var d wsDecision
	select {
	case d = <-p.decision:
	case <-r.Context().Done():
		// The client gave up before the application made up its mind: a
		// later accept fails, one already made is settled here.
		p.mu.Lock()
		p.decided, p.gone = true, true
		p.closed.Store(true)
		select {
		case d := <-p.decision:
			if d.accept {
				d.fut.resolveNone()
			}
		default:
		}
		p.mu.Unlock()
		p.accepted.set()
		return
	}
	if !d.accept {
		writeFull(rw, d.status, d.headers, d.body)
		return
	}

	conn, err := w.upgradeWebsocket(rw, r, d.subprotocol, d.headers)
	if err != nil {
		// the connection was closed before the upgrade
		p.closed.Store(true)
		p.accepted.set()
		d.fut.resolveNone()
		return
	}
	p.mu.Lock()
	p.conn = conn
	closed := p.closed.Load()
	p.mu.Unlock()
	if closed {
		conn.close(wsCloseNormal, "", nil)
	}
	p.accepted.set()
	d.fut.resolveNone()

	w.ws.add(p)
	<-conn.done
	w.ws.remove(p)
	p.closed.Store(true)
}

// goingAway implements wsSession.
func (p *asgiWS) goingAway() {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn != nil {
		conn.close(wsCloseGoingAway, "", nil)
	}
}

// appDone implements appWatcher.
func (p *asgiWS) appDone() {
	p.mu.Lock()
	conn := p.conn
	if !p.decided {
		// the application returned without accepting: handshake denied
		p.rejectLocked(http.StatusForbidden, nil, nil)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	if conn != nil && !p.closed.Swap(true) {
		conn.close(wsCloseNormal, "", nil)
	}
}

// rejectLocked denies the handshake. Decisions are only ever made with p.mu
// held, so the handler can't miss one when the client leaves meanwhile; the
// channel holds the single decision, the send never blocks.
func (p *asgiWS) rejectLocked(status int, headers []headerKV, body []byte) {
	p.decided = true
	p.closed.Store(true)
	p.decision <- wsDecision{status: status, headers: headers, body: body}
	p.accepted.set()
}

func flowError(msg string) pyObj {
	return pyRaise(C.GC_ERR_RUNTIME, "ASGI flow error: "+msg)
}

// ---- receive ---------------------------------------------------------------

func (p *asgiWS) receive() pyObj {
	if !p.initRx {
		// the first event is always the connection request
		p.initRx = true
		if p.closed.Load() {
			return C.balius_done(C.balius_asgi_msg_ws_disconnect(wsCloseNoStatus))
		}
		return C.balius_done(C.balius_asgi_msg_ws_connect())
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	go func() {
		// wait for the application to accept (or reject) the handshake
		<-p.accepted.ch
		p.mu.Lock()
		conn := p.conn
		p.mu.Unlock()

		if conn == nil || p.closed.Load() {
			if p.closed.Load() {
				fut.resolveWith(func() pyObj {
					return C.balius_asgi_msg_ws_disconnect(wsCloseNoStatus)
				})
			} else {
				fut.fail(C.GC_ERR_RUNTIME, "ASGI flow error: Transport not initialized or closed")
			}
			return
		}
		in := conn.read()
		switch {
		case in.closed:
			p.closed.Store(true)
			code := C.int(in.closeCode)
			fut.resolveWith(func() pyObj { return C.balius_asgi_msg_ws_disconnect(code) })
		case in.text:
			fut.resolveWith(func() pyObj {
				cp, cn := cbuf(in.data)
				return C.balius_asgi_msg_ws_text(cp, cn)
			})
		default:
			fut.resolveWith(func() pyObj {
				cp, cn := cbuf(in.data)
				return C.balius_asgi_msg_ws_bytes(cp, cn)
			})
		}
	}()
	return obj
}

// ---- send ------------------------------------------------------------------

// errClientGone is raised on sends after the client left (ASGI 2.4).
const errClientGone = "Websocket connection closed by the client"

func (p *asgiWS) accept(subprotocol string, headers []headerKV) pyObj {
	p.mu.Lock()
	if p.gone {
		p.mu.Unlock()
		return pyRaise(C.GC_ERR_OS, errClientGone)
	}
	if p.decided {
		p.mu.Unlock()
		return flowError("Connection already upgraded")
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		p.mu.Unlock()
		return nil
	}
	p.decided = true
	p.decision <- wsDecision{accept: true, subprotocol: subprotocol, headers: headers, fut: fut}
	p.mu.Unlock()
	return obj
}

// rejectStart handles `websocket.http.response.start`.
func (p *asgiWS) rejectStart(status int, headers []headerKV) pyObj {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.decided {
		return flowError("Connection already upgraded")
	}
	if p.intent != nil {
		return flowError("Response already started")
	}
	p.intent = &response{status: status, headers: headers}
	return pyEmpty()
}

// rejectBody handles `websocket.http.response.body`.
func (p *asgiWS) rejectBody(body []byte, more bool) pyObj {
	if more {
		return unsupportedASGIMessage()
	}
	p.mu.Lock()
	intent := p.intent
	if intent == nil {
		p.mu.Unlock()
		return flowError("Response not initialised")
	}
	if p.decided {
		p.mu.Unlock()
		return flowError("Connection already upgraded")
	}
	p.intent = nil
	p.rejectLocked(intent.status, intent.headers, body)
	p.mu.Unlock()
	return pyEmpty()
}

func (p *asgiWS) sendMessage(text bool, data []byte) pyObj {
	p.mu.Lock()
	conn, decided, gone := p.conn, p.decided, p.gone
	p.mu.Unlock()
	// Sending once the client is gone raises, as ASGI 2.4 specifies: a
	// silent no-op would let a producer loop spin on the event loop forever.
	if gone || (conn != nil && conn.peerGone.Load()) {
		return pyRaise(C.GC_ERR_OS, errClientGone)
	}
	if !decided {
		return flowError("Transport not initialized")
	}
	if p.closed.Load() || conn == nil {
		return pyEmpty()
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	kind := wsBinary
	if text {
		kind = wsText
	}
	conn.send(kind, data, func(err error) {
		if err != nil && conn.peerGone.Load() {
			fut.fail(C.GC_ERR_OS, errClientGone)
			return
		}
		if err != nil {
			logx.Infof("Attempted to write to a closed websocket")
		}
		fut.resolveNone()
	})
	return obj
}

func (p *asgiWS) close(code int, reason string) pyObj {
	p.mu.Lock()
	conn := p.conn
	if !p.decided {
		// closing before accepting denies the handshake
		p.rejectLocked(http.StatusForbidden, nil, nil)
		p.mu.Unlock()
		return pyEmpty()
	}
	p.mu.Unlock()
	p.closed.Store(true)

	if conn == nil {
		// accept in flight: the handler closes right after upgrading
		return pyEmpty()
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	conn.close(code, reason, func(error) { fut.resolveNone() })
	return obj
}

// ---- exports ---------------------------------------------------------------

//export goAsgiWSAccept
func goAsgiWSAccept(h C.uintptr_t, sub *C.char, slen C.Py_ssize_t, hdr *C.char, hlen C.Py_ssize_t, nh C.int) pyObj {
	p, ok := handles.get(uintptr(h)).(*asgiWS)
	if !ok {
		return unsupportedASGIMessage()
	}
	subprotocol := ""
	if sub != nil && slen > 0 {
		subprotocol = C.GoStringN(sub, C.int(slen))
	}
	return p.accept(subprotocol, parseHeaders(unsafe.Pointer(hdr), int(hlen), int(nh)))
}

//export goAsgiWSClose
func goAsgiWSClose(h C.uintptr_t, code C.int, reason *C.char, rlen C.Py_ssize_t) pyObj {
	p, ok := handles.get(uintptr(h)).(*asgiWS)
	if !ok {
		return unsupportedASGIMessage()
	}
	return p.close(int(code), C.GoStringN(reason, C.int(rlen)))
}

//export goAsgiWSSend
func goAsgiWSSend(h C.uintptr_t, text C.int, data *C.char, dlen C.Py_ssize_t) pyObj {
	p, ok := handles.get(uintptr(h)).(*asgiWS)
	if !ok {
		return unsupportedASGIMessage()
	}
	return p.sendMessage(text != 0, C.GoBytes(unsafe.Pointer(data), C.int(dlen)))
}
