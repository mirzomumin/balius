package worker

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/mirzomumin/tulpar/internal/static"
)

// isWebsocketUpgrade tells whether a request asks to switch to websockets.
func isWebsocketUpgrade(r *http.Request) bool {
	return r.ProtoMajor == 1 && r.Method == http.MethodGet &&
		headerHasToken(r.Header, "Connection", "upgrade") &&
		headerHasToken(r.Header, "Upgrade", "websocket")
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h[name] {
		for item := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.TrimSpace(item), token) {
				return true
			}
		}
	}
	return false
}

// checkWebsocketHandshake validates the handshake before the application
// gets involved; it returns the reason of a failure.
func checkWebsocketHandshake(r *http.Request) string {
	if r.Header.Get("Sec-Websocket-Key") == "" {
		return `Missing "Sec-WebSocket-Key"`
	}
	if r.Header.Get("Sec-Websocket-Version") != "13" {
		return `Missing or unsupported "Sec-WebSocket-Version"`
	}
	return ""
}

// Message kinds and close codes, as used by the protocol handlers.
const (
	wsText   = websocket.MessageText
	wsBinary = websocket.MessageBinary

	// RFC 6455 close codes (untyped: they also cross into C)
	wsCloseNormal    = 1000
	wsCloseGoingAway = 1001
	wsCloseNoStatus  = 1005
)

// Largest message accepted from a client.
const wsReadLimit = 64 << 20

// upgradeWebsocket completes the handshake and takes the connection over.
//
// The subprotocol is only confirmed when the client offered it, as RFC 6455
// requires (a client must fail the connection on any other value).
func upgradeWebsocket(rw http.ResponseWriter, r *http.Request, subprotocol string, extra []headerKV) (*wsConn, error) {
	h := rw.Header()
	h.Set("Server", static.ServerName)
	serverSet := false
	for _, kv := range extra {
		k := http.CanonicalHeaderKey(kv.k)
		// the handshake headers are the library's to set
		if strings.HasPrefix(k, "Sec-Websocket-") || k == "Connection" || k == "Upgrade" {
			continue
		}
		if k == "Server" && !serverSet {
			h[k] = nil
			serverSet = true
		}
		h[k] = append(h[k], kv.v)
	}
	opts := &websocket.AcceptOptions{
		// origin policies are up to the application
		InsecureSkipVerify: true,
		CompressionMode:    websocket.CompressionDisabled,
	}
	if subprotocol != "" {
		opts.Subprotocols = []string{subprotocol}
	}
	conn, err := websocket.Accept(rw, r, opts)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(wsReadLimit)
	c := &wsConn{
		conn:      conn,
		ctx:       context.Background(),
		closeCode: wsCloseNoStatus,
		out:       newMailbox[wsOut](),
		inbox:     make(chan wsIn, wsInboxSize),
		done:      make(chan struct{}),
	}
	go c.writeLoop()
	go c.readLoop()
	return c, nil
}

// wsOut is a queued outgoing frame: a data message, or the close frame.
type wsOut struct {
	kind    websocket.MessageType
	data    []byte
	close   bool
	code    int
	reason  string
	onWrite func(error) // optional
}

var errWSClosed = errors.New("websocket is closed")

// wsConn is an established websocket connection.
//
// Outgoing frames go through an ordered queue drained by a single writer
// goroutine, so the order in which the application called `send` is the
// order on the wire, awaited or not.
//
// Incoming frames are read continuously by a reader goroutine into a small
// inbox: control frames (pings, close) get answered even while the
// application isn't receiving, and a full inbox stops the reads, which is
// how backpressure reaches the client.
type wsConn struct {
	conn  *websocket.Conn
	ctx   context.Context
	out   *mailbox[wsOut]
	inbox chan wsIn

	peerGone  atomic.Bool // the reader hit the end of the stream
	closeCode int         // why: set before the inbox is closed
	broken    atomic.Bool // writes can't succeed anymore
	closing   atomic.Bool // a close frame was queued
	over      sync.Once
	done      chan struct{} // closed once the connection is gone
}

func (c *wsConn) writeLoop() {
	var batch []wsOut
	for {
		<-c.out.notify
		var closed bool
		batch, closed = c.out.take(batch)
		for i := range batch {
			item := &batch[i]
			switch {
			case c.broken.Load():
				if item.onWrite != nil {
					item.onWrite(errWSClosed)
				}
			case item.close:
				c.broken.Store(true)
				// Everything queued before is on the wire: the close is
				// committed. Close then waits (bounded) for the peer's reply,
				// which the reader goroutine consumes.
				if item.onWrite != nil {
					item.onWrite(nil)
				}
				_ = c.conn.Close(websocket.StatusCode(item.code), item.reason)
				c.kill()
			default:
				err := c.conn.Write(c.ctx, item.kind, item.data)
				if err != nil {
					c.broken.Store(true)
				}
				if item.onWrite != nil {
					item.onWrite(err)
				}
			}
			batch[i] = wsOut{}
		}
		if closed {
			return
		}
	}
}

// send queues a data frame.
func (c *wsConn) send(kind websocket.MessageType, data []byte, onWrite func(error)) {
	if !c.out.push(wsOut{kind: kind, data: data, onWrite: onWrite}) && onWrite != nil {
		onWrite(errWSClosed)
	}
}

// close starts the closing handshake: a close frame is sent after whatever
// is already queued, then the peer gets a moment to acknowledge it.
func (c *wsConn) close(code int, reason string, onWrite func(error)) {
	if !c.closing.CompareAndSwap(false, true) {
		if onWrite != nil {
			onWrite(nil)
		}
		return
	}
	if len(reason) > 123 {
		reason = reason[:123] // control frames carry at most 125 bytes
	}
	if !c.out.push(wsOut{close: true, code: code, reason: reason, onWrite: onWrite}) && onWrite != nil {
		onWrite(nil)
	}
	c.out.close()
}

// kill drops the connection right away.
func (c *wsConn) kill() {
	c.over.Do(func() {
		c.broken.Store(true)
		c.closing.Store(true)
		c.out.close()
		_ = c.conn.CloseNow()
		close(c.done)
	})
}

// wsIn is an incoming message, or the reason there won't be any more.
type wsIn struct {
	text      bool
	data      []byte
	closed    bool // the connection is over
	closeCode int  // meaningful when closed
}

// Messages read ahead of the application.
const wsInboxSize = 16

func (c *wsConn) readLoop() {
	defer close(c.inbox)
	for {
		kind, data, err := c.conn.Read(c.ctx)
		if err != nil {
			code := int(websocket.CloseStatus(err))
			if code < 0 {
				code = wsCloseNoStatus
			}
			// The library already answered the close frame, if there was one.
			// Closing the inbox (deferred) delivers the news without blocking
			// on an application that stopped receiving.
			c.closeCode = code
			c.peerGone.Store(true)
			go c.kill() // may wait for a close handshake in progress
			return
		}
		select {
		case c.inbox <- wsIn{text: kind == websocket.MessageText, data: data}:
		case <-c.done:
			return
		}
	}
}

// read waits for the next data message. Once it reports closed the
// connection has been dropped.
func (c *wsConn) read() wsIn {
	in, ok := <-c.inbox
	if !ok {
		return wsIn{closed: true, closeCode: c.closeCode}
	}
	return in
}

// wsSession is an accepted websocket connection as seen by the worker.
type wsSession interface {
	// goingAway is called when the worker shuts down.
	goingAway()
}

// wsRegistry tracks the accepted websocket connections: once hijacked they
// are invisible to http.Server.Shutdown.
type wsRegistry struct {
	mu       sync.Mutex
	sessions map[wsSession]struct{}
	empty    chan struct{}
}

func newWSRegistry() *wsRegistry {
	return &wsRegistry{sessions: map[wsSession]struct{}{}}
}

func (r *wsRegistry) add(s wsSession) {
	r.mu.Lock()
	r.sessions[s] = struct{}{}
	r.mu.Unlock()
}

func (r *wsRegistry) remove(s wsSession) {
	r.mu.Lock()
	delete(r.sessions, s)
	if len(r.sessions) == 0 && r.empty != nil {
		close(r.empty)
		r.empty = nil
	}
	r.mu.Unlock()
}

func (r *wsRegistry) closeAll() {
	r.mu.Lock()
	sessions := make([]wsSession, 0, len(r.sessions))
	for s := range r.sessions {
		sessions = append(sessions, s)
	}
	r.mu.Unlock()
	for _, s := range sessions {
		s.goingAway()
	}
}

// wait blocks until every session is gone, or the timeout expires.
func (r *wsRegistry) wait(timeout time.Duration) {
	r.mu.Lock()
	if len(r.sessions) == 0 {
		r.mu.Unlock()
		return
	}
	if r.empty == nil {
		r.empty = make(chan struct{})
	}
	empty := r.empty
	r.mu.Unlock()
	select {
	case <-empty:
	case <-time.After(timeout):
	}
}
