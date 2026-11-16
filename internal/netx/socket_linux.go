package netx

import "golang.org/x/sys/unix"

// ReusePortBalanced: Linux distributes connections across all the sockets
// bound with SO_REUSEPORT to the same address, so every worker can open its
// own listening socket.
const ReusePortBalanced = true

func socket(domain, typ, proto int) (int, error) {
	return unix.Socket(domain, typ|unix.SOCK_CLOEXEC, proto)
}
