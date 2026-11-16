package worker

import (
	"errors"
	"io"
	"sync"
)

// prefetchBody reads a request body ahead of its consumer, into memory.
//
// HTTP/2 flow control is shared by every stream of a connection: a body
// nobody reads keeps holding connection window. When the application runs
// on a bounded thread pool (WSGI), requests queued for a thread would
// starve the running ones of window, and the connection deadlocks.
// Draining the body as it arrives returns the window right away.
type prefetchBody struct {
	src io.ReadCloser

	mu   sync.Mutex
	cond *sync.Cond
	buf  []byte
	err  error // set once the source is exhausted (io.EOF) or failed
	done chan struct{}
}

func newPrefetchBody(src io.ReadCloser) *prefetchBody {
	p := &prefetchBody{src: src, done: make(chan struct{})}
	p.cond = sync.NewCond(&p.mu)
	go p.fill()
	return p
}

func (p *prefetchBody) fill() {
	defer close(p.done)
	chunk := make([]byte, 32*1024)
	for {
		n, err := p.src.Read(chunk)
		p.mu.Lock()
		p.buf = append(p.buf, chunk[:n]...)
		if err != nil {
			p.err = err
		}
		p.cond.Broadcast()
		p.mu.Unlock()
		if err != nil {
			return
		}
	}
}

func (p *prefetchBody) Read(dst []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.buf) == 0 && p.err == nil {
		p.cond.Wait()
	}
	if len(p.buf) == 0 {
		return 0, p.err
	}
	n := copy(dst, p.buf)
	p.buf = p.buf[n:]
	if len(p.buf) == 0 {
		p.buf = nil
	}
	return n, nil
}

// Close stops the prefetching and waits for it to be over: the source must
// not be read once the handler returns.
func (p *prefetchBody) Close() error {
	err := p.src.Close()
	<-p.done
	p.mu.Lock()
	if p.err == nil || errors.Is(p.err, io.EOF) {
		p.err = io.ErrClosedPipe
	}
	p.cond.Broadcast()
	p.mu.Unlock()
	return err
}
