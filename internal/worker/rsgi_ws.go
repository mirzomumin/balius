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

	"github.com/mirzomumin/tulpar/internal/logx"
)

// rsgiWS is the state of one RSGI websocket connection.
type rsgiWS struct {
	w *Worker

	mu       sync.Mutex
	decided  bool    // accept() or close() settled the handshake
	finished bool    // close() was called (explicitly or by the app returning)
	conn     *wsConn // set once upgraded
	decision chan wsDecision

	closed         atomic.Bool
	closeDelivered atomic.Bool
}

func (w *Worker) serveRSGIWebsocket(rw http.ResponseWriter, r *http.Request, ci *connInfo) {
	if reason := checkWebsocketHandshake(r); reason != "" {
		logx.Infof("Websocket handshake failed with %s", reason)
		writeError(rw, http.StatusBadRequest, reason)
		return
	}
	p := &rsgiWS{w: w, decision: make(chan wsDecision, 1)}
	w.dispatch(true, C.GC_KIND_WS, p, r, ci)

	var d wsDecision
	select {
	case d = <-p.decision:
	case <-r.Context().Done():
		// The client left before the application decided: a later accept
		// fails, one already made is settled here.
		p.mu.Lock()
		p.decided = true
		p.closed.Store(true)
		select {
		case d := <-p.decision:
			if d.accept {
				w.failAccept(d)
			}
		default:
		}
		p.mu.Unlock()
		return
	}
	if !d.accept {
		writeFull(rw, d.status, nil, nil)
		return
	}

	conn, err := upgradeWebsocket(rw, r, "", nil)
	if err != nil {
		p.closed.Store(true)
		w.failAccept(d)
		return
	}
	p.mu.Lock()
	p.conn = conn
	finished := p.finished
	p.mu.Unlock()
	if finished {
		conn.close(wsCloseNormal, "", nil)
	}
	self := d.self
	d.fut.resolveWith(func() pyObj {
		transport := C.tulpar_rsgi_ws_transport(self)
		C.tulpar_decref(self)
		return transport
	})

	w.ws.add(p)
	<-conn.done
	w.ws.remove(p)
	p.closed.Store(true)
}

// failAccept fails the future of an accept that won't happen, and drops the
// protocol reference the accept took.
func (w *Worker) failAccept(d wsDecision) {
	w.loop.Submit(func() {
		C.tulpar_future_fail(d.fut.obj, C.GC_ERR_RSGI_PROTO, cmsg(rsgiProtocolError))
		C.tulpar_decref(d.self)
	})
}

// goingAway implements wsSession.
func (p *rsgiWS) goingAway() {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn != nil {
		conn.close(wsCloseGoingAway, "", nil)
	}
}

// appDone implements appWatcher: equivalent to `protocol.close()`.
func (p *rsgiWS) appDone() { p.close(-1) }

func (p *rsgiWS) close(status int) {
	p.mu.Lock()
	if p.finished {
		p.mu.Unlock()
		return
	}
	p.finished = true
	conn := p.conn
	if !p.decided {
		// decisions are made with p.mu held: see the handler
		p.decided = true
		if status < 100 || status > 999 {
			status = http.StatusForbidden
		}
		p.decision <- wsDecision{status: status}
		p.mu.Unlock()
		p.closed.Store(true)
		return
	}
	p.mu.Unlock()
	p.closed.Store(true)

	if conn != nil {
		conn.close(wsCloseNormal, "", nil)
	}
}

func (p *rsgiWS) accept(self pyObj) pyObj {
	p.mu.Lock()
	if p.decided {
		p.mu.Unlock()
		return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		p.mu.Unlock()
		return nil
	}
	p.decided = true
	// the transport handed to the application keeps the protocol alive
	C.tulpar_incref(self)
	p.decision <- wsDecision{accept: true, fut: fut, self: self}
	p.mu.Unlock()
	return obj
}

func (p *rsgiWS) receive() pyObj {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn == nil {
		return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
	}
	fut, obj, ok := p.w.loop.newFuture()
	if !ok {
		return nil
	}
	go func() {
		in := conn.read()
		switch {
		case in.closed && p.closeDelivered.Swap(true):
			// the close message was already handed out
			fut.fail(C.GC_ERR_RSGI_CLOSED, rsgiClosedError)
		case in.closed:
			p.closed.Store(true)
			fut.resolveWith(func() pyObj { return C.tulpar_rsgi_ws_message(0, nil, 0) })
		case in.text:
			fut.resolveWith(func() pyObj {
				cp, cn := cbuf(in.data)
				return C.tulpar_rsgi_ws_message(2, cp, cn)
			})
		default:
			fut.resolveWith(func() pyObj {
				cp, cn := cbuf(in.data)
				return C.tulpar_rsgi_ws_message(1, cp, cn)
			})
		}
	}()
	return obj
}

func (p *rsgiWS) send(text bool, data []byte) pyObj {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()
	if conn == nil || p.closed.Load() {
		return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
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
		if err != nil {
			fut.fail(C.GC_ERR_RSGI_CLOSED, rsgiClosedError)
			return
		}
		fut.resolveNone()
	})
	return obj
}

// ---- exports ---------------------------------------------------------------

//export goRsgiWSAccept
func goRsgiWSAccept(h C.uintptr_t, self pyObj) pyObj {
	if p, ok := handles.get(uintptr(h)).(*rsgiWS); ok {
		return p.accept(self)
	}
	return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
}

//export goRsgiWSClose
func goRsgiWSClose(h C.uintptr_t, status C.int) {
	if p, ok := handles.get(uintptr(h)).(*rsgiWS); ok {
		p.close(int(status))
	}
}

//export goRsgiWSReceive
func goRsgiWSReceive(h C.uintptr_t) pyObj {
	if p, ok := handles.get(uintptr(h)).(*rsgiWS); ok {
		return p.receive()
	}
	return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
}

//export goRsgiWSSend
func goRsgiWSSend(h C.uintptr_t, text C.int, data *C.char, dlen C.Py_ssize_t) pyObj {
	if p, ok := handles.get(uintptr(h)).(*rsgiWS); ok {
		return p.send(text != 0, C.GoBytes(unsafe.Pointer(data), C.int(dlen)))
	}
	return pyRaise(C.GC_ERR_RSGI_PROTO, rsgiProtocolError)
}
