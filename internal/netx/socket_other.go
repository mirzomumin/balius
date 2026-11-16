//go:build !linux

package netx

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// ReusePortBalanced: on the BSDs and macOS SO_REUSEPORT does not balance
// connections (the last socket bound wins), so workers share one listening
// socket.
const ReusePortBalanced = false

// socket creates a close-on-exec socket where SOCK_CLOEXEC doesn't exist:
// the fork lock keeps a concurrent exec from inheriting it in between.
func socket(domain, typ, proto int) (int, error) {
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	fd, err := unix.Socket(domain, typ, proto)
	if err == nil {
		unix.CloseOnExec(fd)
	}
	return fd, err
}
