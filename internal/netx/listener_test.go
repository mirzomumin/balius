package netx

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type stats struct{ accepted, closed, errs atomic.Int32 }

func (s *stats) ConnAccepted() { s.accepted.Add(1) }
func (s *stats) ConnClosed()   { s.closed.Add(1) }
func (s *stats) ConnError()    { s.errs.Add(1) }

func TestBackpressure(t *testing.T) {
	inner, err := ListenTCP("127.0.0.1", 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	st := &stats{}
	l := NewBackpressure(inner, 1, st)
	defer l.Close()
	addr := inner.Addr().String()

	accepted := make(chan net.Conn, 2)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()
	c1, _ := net.Dial("tcp", addr)
	defer c1.Close()
	c2, _ := net.Dial("tcp", addr)
	defer c2.Close()

	first := <-accepted
	select {
	case <-accepted:
		t.Fatal("a second connection was accepted beyond the limit")
	case <-time.After(200 * time.Millisecond):
	}
	first.Close()
	first.Close() // idempotent
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("the queued connection was not accepted once a slot freed up")
	}
	if st.accepted.Load() != 2 || st.closed.Load() != 1 {
		t.Errorf("stats: accepted=%d closed=%d", st.accepted.Load(), st.closed.Load())
	}
}

func TestTCPSocketResolvesPort(t *testing.T) {
	f, port, err := TCPSocket("127.0.0.1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if port == 0 {
		t.Fatal("port 0 was not resolved")
	}
	// SO_REUSEPORT: a worker can bind the same address
	l, err := ListenTCP("127.0.0.1", port, 16)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}

func TestUnixSocketPathTooLong(t *testing.T) {
	long := "/tmp/" + string(make([]byte, 200))
	if _, err := UnixSocket(long, -1); err == nil {
		t.Error("expected an error for an over-long path")
	}
}
