package worker

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/mirzomumin/balius/internal/static"
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
	wsCloseNormal        = 1000
	wsCloseGoingAway     = 1001
	wsCloseNoStatus      = 1005
	wsCloseInternalError = 1011
)

// upgradeWebsocket completes the handshake and takes the connection over.
//
// The subprotocol is only confirmed when the client offered it, as RFC 6455
// requires (a client must fail the connection on any other value).
func (w *Worker) upgradeWebsocket(rw http.ResponseWriter, r *http.Request, subprotocol string, extra []headerKV) (*wsConn, error) {
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
	if w.cfg.WSCompression {
		// per-message compression without a window kept between messages:
		// no memory held per connection, small messages are left alone
		opts.CompressionMode = websocket.CompressionNoContextTakeover
	}
	if subprotocol != "" {
		opts.Subprotocols = []string{subprotocol}
	}
	hijacker := &corkHijacker{ResponseWriter: rw}
	conn, err := websocket.Accept(hijacker, r, opts)
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(int64(w.cfg.WSMaxSize))
	c := &wsConn{
		conn:      conn,
		cork:      hijacker.conn,
		ctx:       context.Background(),
		closeCode: wsCloseNoStatus,
		out:       newMailbox[wsOut](),
		inbox:     make(chan wsIn, wsInboxSize),
		done:      make(chan struct{}),
	}
	go c.writeLoop()
	go c.readLoop()
	if interval := seconds(w.cfg.WSPingInterval); interval > 0 {
		go c.pingLoop(interval, seconds(w.cfg.WSPingTimeout))
	}
	return c, nil
}

func seconds(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

// pingLoop keeps the connection alive through idle proxies and detects
// peers that vanished without closing: a ping goes out every interval and
// the connection is closed when its pong doesn't come back within timeout
// (zero waits forever). Pongs are read by readLoop, so a client flooding an
// application that stopped receiving looks dead too.
func (c *wsConn) pingLoop(interval, timeout time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-tick.C:
		}
		ctx, cancel := c.ctx, context.CancelFunc(func() {})
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(c.ctx, timeout)
		}
		err := c.conn.Ping(ctx)
		cancel()
		if err != nil {
			c.close(wsCloseInternalError, "keepalive ping timeout", nil)
			return
		}
	}
}

// corkConn is the connection under a websocket. While corked, writes are
// collected and go out as a single one: a burst of frames costs one system
// call (and fewer packets) instead of one per frame.
type corkConn struct {
	net.Conn
	mu     sync.Mutex
	corked bool
	buf    []byte
}

// More than this is not worth holding back.
const corkLimit = 64 << 10

func (c *corkConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.corked {
		return c.Conn.Write(p)
	}
	c.buf = append(c.buf, p...)
	if len(c.buf) < corkLimit {
		return len(p), nil
	}
	return len(p), c.flush()
}

func (c *corkConn) flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	_, err := c.Conn.Write(c.buf)
	c.buf = c.buf[:0]
	return err
}

func (c *corkConn) cork() {
	c.mu.Lock()
	c.corked = true
	c.mu.Unlock()
}

// uncork sends what was held back.
func (c *corkConn) uncork() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.corked = false
	return c.flush()
}

// corkHijacker makes the websocket library write through a corkConn.
type corkHijacker struct {
	http.ResponseWriter
	conn *corkConn
}

func (h *corkHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, brw, err := http.NewResponseController(h.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	h.conn = &corkConn{Conn: conn}
	return h.conn, brw, nil
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
	cork  *corkConn
	ctx   context.Context
	out   *mailbox[wsOut]
	inbox chan wsIn

	peerGone  atomic.Bool // the reader hit the end of the stream
	closeCode int         // why: set before the inbox is closed
	broken    atomic.Bool // writes can't succeed anymore
	closing   atomic.Bool // a close frame was queued
	streak    int         // calls in a row that didn't suspend the application
	over      sync.Once
	done      chan struct{} // closed once the connection is gone
}

func (c *wsConn) writeLoop() {
	var (
		batch []wsOut
		held  []func(error) // callbacks of the frames waiting in the cork
	)
	// uncork sends the frames held back and tells their senders
	uncork := func() {
		err := c.cork.uncork()
		if err != nil {
			c.broken.Store(true)
		}
		for i, onWrite := range held {
			onWrite(err)
			held[i] = nil
		}
		held = held[:0]
	}
	for {
		<-c.out.notify
		var closed bool
		batch, closed = c.out.take(batch)
		// several frames queued: one write for all of them
		corked := len(batch) > 1
		if corked {
			c.cork.cork()
		}
		for i := range batch {
			item := &batch[i]
			switch {
			case c.broken.Load():
				if item.onWrite != nil {
					item.onWrite(errWSClosed)
				}
			case item.close:
				if corked {
					uncork()
					corked = false
				}
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
				switch {
				case item.onWrite == nil:
				case corked && err == nil:
					held = append(held, item.onWrite)
				default:
					item.onWrite(err)
				}
			}
			batch[i] = wsOut{}
		}
		if corked {
			uncork()
		}
		if closed {
			return
		}
	}
}

// Frames an application may have queued before `send` makes it wait for
// the writer: awaiting every single write costs a round trip through the
// event loop per message.
const (
	wsSendWindow    = 16
	wsSendWindowMax = 64 << 10 // larger messages always wait
)

// A coroutine whose sends and receives all complete on the spot never
// yields to the event loop, starving every other task: after this many in
// a row the next one takes the slow path, which suspends it.
const wsFastStreak = 16

// fast accounts for a call about to complete without suspending the
// coroutine, unless it is time to yield. Loop thread only, like streak.
func (c *wsConn) fast() bool {
	if c.streak >= wsFastStreak {
		c.streak = 0
		return false
	}
	c.streak++
	return true
}

// trySend queues a data frame without waiting for it to be written. It
// reports false when the sender has to wait instead (see send): the queue
// is long, the message is large, or the connection is broken and the
// sender must learn about it.
func (c *wsConn) trySend(kind websocket.MessageType, data []byte) bool {
	if len(data) > wsSendWindowMax || c.broken.Load() || c.out.size() >= wsSendWindow || !c.fast() {
		c.streak = 0
		return false
	}
	return c.out.push(wsOut{kind: kind, data: data})
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

// tryRead returns a data message that was already received, if any.
func (c *wsConn) tryRead() (wsIn, bool) {
	if len(c.inbox) > 0 && c.fast() {
		select {
		case in, ok := <-c.inbox:
			return in, ok
		default:
		}
	}
	c.streak = 0
	return wsIn{}, false
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
