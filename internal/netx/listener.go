// Package netx builds the listening sockets of balius and implements the
// per-worker backpressure on accepted connections.
package netx

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"

	"golang.org/x/sys/unix"
)

func tcpSocket(host string, port int) (fd int, sa unix.Sockaddr, err error) {
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return -1, nil, fmt.Errorf("invalid host address %q: %w", host, err)
	}
	domain := unix.AF_INET
	if ip.Is4() {
		sa = &unix.SockaddrInet4{Port: port, Addr: ip.As4()}
	} else {
		domain = unix.AF_INET6
		sa6 := &unix.SockaddrInet6{Port: port, Addr: ip.As16()}
		if zone := ip.Zone(); zone != "" {
			if ifi, err := net.InterfaceByName(zone); err == nil {
				sa6.ZoneId = uint32(ifi.Index)
			}
		}
		sa = sa6
	}
	fd, err = socket(domain, unix.SOCK_STREAM, unix.IPPROTO_TCP)
	if err != nil {
		return -1, nil, fmt.Errorf("socket: %w", err)
	}
	for _, opt := range []int{unix.SO_REUSEPORT, unix.SO_REUSEADDR} {
		if err = unix.SetsockoptInt(fd, unix.SOL_SOCKET, opt, 1); err != nil {
			unix.Close(fd)
			return -1, nil, fmt.Errorf("setsockopt: %w", err)
		}
	}
	_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_NODELAY, 1)
	return fd, sa, nil
}

// TCPSocket creates a TCP socket bound to host:port with SO_REUSEPORT and
// SO_REUSEADDR set. The socket is NOT listening: the supervisor uses it to
// reserve (and resolve) the port, workers call Listen on their own copy.
// It returns the socket along with the port actually bound.
func TCPSocket(host string, port int) (*os.File, int, error) {
	fd, sa, err := tcpSocket(host, port)
	if err != nil {
		return nil, 0, err
	}
	if err := unix.Bind(fd, sa); err != nil {
		unix.Close(fd)
		return nil, 0, fmt.Errorf("bind %s: %w", net.JoinHostPort(host, fmt.Sprint(port)), err)
	}
	bound, err := unix.Getsockname(fd)
	if err != nil {
		unix.Close(fd)
		return nil, 0, fmt.Errorf("getsockname: %w", err)
	}
	switch a := bound.(type) {
	case *unix.SockaddrInet4:
		port = a.Port
	case *unix.SockaddrInet6:
		port = a.Port
	}
	return os.NewFile(uintptr(fd), "balius-tcp"), port, nil
}

// UnixSocket creates a socket bound to the given filesystem path. A stale
// socket file left behind by a previous run is replaced. perm < 0 leaves
// the default permissions.
func UnixSocket(path string, perm int) (*os.File, error) {
	// sun_path holds 108 bytes on Linux (104 on the BSDs), terminator included
	if len(path) >= len(unix.RawSockaddrUnix{}.Path) {
		return nil, fmt.Errorf("unix socket path %s is too long (max %d bytes)", path, len(unix.RawSockaddrUnix{}.Path)-1)
	}
	if st, err := os.Lstat(path); err == nil && st.Mode()&os.ModeSocket != 0 {
		if c, err := net.Dial("unix", path); err == nil {
			c.Close()
			return nil, fmt.Errorf("unix socket %s is already in use", path)
		}
		_ = os.Remove(path)
	}
	fd, err := socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("socket: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrUnix{Name: path}); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("bind %s: %w", path, err)
	}
	if perm >= 0 {
		if err := os.Chmod(path, os.FileMode(perm)); err != nil {
			unix.Close(fd)
			return nil, fmt.Errorf("chmod %s: %w", path, err)
		}
	}
	return os.NewFile(uintptr(fd), "balius-uds"), nil
}

// Listen starts listening on a bound socket and wraps it in a net.Listener.
// The listener owns a duplicate of the descriptor, f is left untouched.
func Listen(f *os.File, backlog int) (net.Listener, error) {
	if err := unix.Listen(int(f.Fd()), backlog); err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	l, err := net.FileListener(f)
	if err != nil {
		return nil, err
	}
	if ul, ok := l.(*net.UnixListener); ok {
		// the socket file belongs to the supervisor
		ul.SetUnlinkOnClose(false)
	}
	return l, nil
}

// ListenTCP opens a worker's own SO_REUSEPORT listening socket.
func ListenTCP(host string, port, backlog int) (net.Listener, error) {
	f, _, err := TCPSocket(host, port)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Listen(f, backlog)
}

// ConnStats receives connection lifecycle events.
type ConnStats interface {
	ConnAccepted()
	ConnClosed()
	ConnError()
}

// Backpressure wraps a listener so that at most `limit` accepted
// connections are alive at any time: the accept loop stops taking
// connections from the socket queue until one of them is closed.
type Backpressure struct {
	net.Listener
	sem    chan struct{}
	closed chan struct{}
	once   sync.Once
	stats  ConnStats
}

// NewBackpressure wraps l. stats may be nil.
func NewBackpressure(l net.Listener, limit int, stats ConnStats) *Backpressure {
	if limit < 1 {
		limit = 1
	}
	return &Backpressure{
		Listener: l,
		sem:      make(chan struct{}, limit),
		closed:   make(chan struct{}),
		stats:    stats,
	}
}

// Accept waits for a free slot, then for a connection.
func (b *Backpressure) Accept() (net.Conn, error) {
	select {
	case b.sem <- struct{}{}:
	case <-b.closed:
		return nil, net.ErrClosed
	}
	c, err := b.Listener.Accept()
	if err != nil {
		<-b.sem
		if b.stats != nil && !errors.Is(err, net.ErrClosed) {
			b.stats.ConnError()
		}
		return nil, err
	}
	if b.stats != nil {
		b.stats.ConnAccepted()
	}
	return &conn{Conn: c, parent: b}, nil
}

// Close stops the accept loop.
func (b *Backpressure) Close() error {
	b.once.Do(func() { close(b.closed) })
	return b.Listener.Close()
}

type conn struct {
	net.Conn
	parent   *Backpressure
	released atomic.Bool
}

func (c *conn) Close() error {
	err := c.Conn.Close()
	if c.released.CompareAndSwap(false, true) {
		<-c.parent.sem
		if c.parent.stats != nil {
			c.parent.stats.ConnClosed()
		}
	}
	return err
}

// CloseWrite lets net/http half-close the connection (FIN before close) when
// it drops a request body it didn't read, so the client still gets the
// response instead of a reset.
func (c *conn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// ReadFrom keeps the sendfile/splice fast path of *net.TCPConn reachable
// through the wrapper.
func (c *conn) ReadFrom(r io.Reader) (int64, error) {
	if rf, ok := c.Conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(struct{ io.Writer }{c.Conn}, r)
}
