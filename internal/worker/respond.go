package worker

import (
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/mirzomumin/balius/internal/logx"
	"github.com/mirzomumin/balius/internal/static"
)

// headerKV is a response header as provided by the application.
type headerKV struct{ k, v string }

var serverHeader = []string{static.ServerName}

// parseHeaders decodes the header pairs serialized by the C glue
// (gc_hbuf_headers). All the names and values share a single allocation.
func parseHeaders(p unsafe.Pointer, size, count int) []headerKV {
	if count == 0 || size == 0 {
		return nil
	}
	data := string(unsafe.Slice((*byte)(p), size))
	out := make([]headerKV, 0, count)
	off := 0
	next := func() string {
		if off+4 > len(data) {
			off = len(data)
			return ""
		}
		n := int(binary.NativeEndian.Uint32(unsafe.Slice(unsafe.StringData(data[off:]), 4)))
		off += 4
		if off+n > len(data) {
			off = len(data)
			return ""
		}
		s := data[off : off+n]
		off += n
		return s
	}
	for i := 0; i < count; i++ {
		k := next()
		v := next()
		out = append(out, headerKV{k, v})
	}
	return out
}

// applyHeaders copies the application headers into the response.
func applyHeaders(dst http.Header, hs []headerKV) {
	for _, h := range hs {
		k := textproto.CanonicalMIMEHeaderKey(h.k)
		dst[k] = append(dst[k], h.v)
	}
	if _, ok := dst["Server"]; !ok {
		dst["Server"] = serverHeader
	}
	if _, ok := dst["Content-Type"]; !ok {
		// never let net/http sniff a content type the application didn't set
		dst["Content-Type"] = nil
	}
}

func headerHasPrefix(hs []headerKV, name, prefix string) bool {
	for _, h := range hs {
		if strings.EqualFold(h.k, name) && strings.HasPrefix(h.v, prefix) {
			return true
		}
	}
	return false
}

func bodyAllowed(status int) bool {
	return status >= 200 && status != http.StatusNoContent && status != http.StatusNotModified
}

// writeFull sends a response whose body is entirely known.
func writeFull(w http.ResponseWriter, status int, hs []headerKV, body []byte) {
	h := w.Header()
	applyHeaders(h, hs)
	if bodyAllowed(status) {
		if _, ok := h["Content-Length"]; !ok {
			h["Content-Length"] = []string{strconv.Itoa(len(body))}
		}
	}
	w.WriteHeader(status)
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
}

// writeError sends one of the server generated error responses.
func writeError(w http.ResponseWriter, status int, body string) {
	h := w.Header()
	h["Server"] = serverHeader
	h["Content-Type"] = nil
	h["Content-Length"] = []string{strconv.Itoa(len(body))}
	w.WriteHeader(status)
	if body != "" {
		_, _ = io.WriteString(w, body)
	}
}

func write500(w http.ResponseWriter) {
	writeError(w, http.StatusInternalServerError, "Internal server error")
}

// writeFile streams a file (or the [start, end) range of it when end > 0).
func writeFile(w http.ResponseWriter, status int, hs []headerKV, path string, start, end int64) {
	f, err := os.Open(path)
	if err != nil {
		logx.Infof("Cannot open file %s", path)
		static.NotFoundResponse(w)
		return
	}
	defer f.Close()

	size := int64(-1)
	if end > 0 {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			logx.Errorf("Cannot seek to position %d in file %s", start, path)
			write500(w)
			return
		}
		size = end - start
	} else if st, err := f.Stat(); err == nil && st.Mode().IsRegular() {
		size = st.Size()
	}

	h := w.Header()
	applyHeaders(h, hs)
	if size >= 0 && bodyAllowed(status) {
		// a known length keeps the response unchunked and sendfile-able
		if _, ok := h["Content-Length"]; !ok {
			h["Content-Length"] = []string{strconv.FormatInt(size, 10)}
		}
	}
	w.WriteHeader(status)
	if size >= 0 {
		_, _ = io.CopyN(w, f, size)
	} else {
		_, _ = io.Copy(w, f)
	}
}

// chunk is a piece of a streamed response body.
type chunk struct {
	data []byte
	last bool
}

// onceSignal is a broadcast flag: set once, waited on by many.
type onceSignal struct {
	ch   chan struct{}
	once sync.Once
}

func newOnceSignal() *onceSignal { return &onceSignal{ch: make(chan struct{})} }

func (s *onceSignal) set() { s.once.Do(func() { close(s.ch) }) }

func (s *onceSignal) isSet() bool {
	select {
	case <-s.ch:
		return true
	default:
		return false
	}
}

// reqBody guards the request body, which net/http only allows reading
// while the handler goroutine is still inside ServeHTTP.
type reqBody struct {
	r     *http.Request
	rc    *http.ResponseController
	empty bool // the request carries no body at all

	mu   sync.Mutex
	over bool
}

var errBodyOver = errors.New("request is over")

func hasBody(r *http.Request) bool {
	return r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0
}

// init prepares reading r's body from the application side.
func (b *reqBody) init(rw http.ResponseWriter, r *http.Request) {
	b.r, b.rc, b.empty = r, http.NewResponseController(rw), !hasBody(r)
	if !b.empty && r.ProtoMajor == 1 {
		// let the application answer while it is still reading the request
		_ = b.rc.EnableFullDuplex()
	}
}

// read reads the next piece of the body. It never returns (0, nil).
func (b *reqBody) read(buf []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.over {
		return 0, errBodyOver
	}
	for {
		n, err := b.r.Body.Read(buf)
		if n > 0 || err != nil {
			return n, err
		}
	}
}

// finish must run before the handler returns: it interrupts a read in
// flight, if any, and forbids further ones.
func (b *reqBody) finish() {
	if !b.mu.TryLock() {
		_ = b.rc.SetReadDeadline(time.Now())
		b.mu.Lock()
	}
	b.over = true
	b.mu.Unlock()
}

// Pooled buffers used to read request bodies.
const bodyBufSize = 128 * 1024

var bodyBufPool = sync.Pool{New: func() any {
	b := make([]byte, bodyBufSize)
	return &b
}}

// mailbox is an unbounded FIFO queue with a single consumer.
type mailbox[T any] struct {
	mu     sync.Mutex
	items  []T
	closed bool
	notify chan struct{}
}

func newMailbox[T any]() *mailbox[T] {
	return &mailbox[T]{notify: make(chan struct{}, 1)}
}

// push appends an item; it reports false when the mailbox is closed.
func (m *mailbox[T]) push(v T) bool {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	m.items = append(m.items, v)
	m.mu.Unlock()
	select {
	case m.notify <- struct{}{}:
	default:
	}
	return true
}

// close marks the end of the stream: queued items are still delivered.
func (m *mailbox[T]) close() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

// take returns everything queued so far, and whether the mailbox is closed.
func (m *mailbox[T]) take(buf []T) ([]T, bool) {
	m.mu.Lock()
	buf = append(buf[:0], m.items...)
	clear(m.items)
	m.items = m.items[:0]
	closed := m.closed
	m.mu.Unlock()
	return buf, closed
}
