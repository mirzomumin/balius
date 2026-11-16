// tulpar is a Go HTTP server for Python applications: a port of granian
// (https://github.com/emmett-framework/granian) from Rust to Go.
//
// The same executable plays two roles. Started by a user it is the
// supervisor: it parses the command line, binds the address and spawns
// workers. Started by the supervisor it is a worker: it embeds the CPython
// interpreter, loads the application and serves requests.
package main

import (
	"os"
	"runtime"

	"github.com/mirzomumin/tulpar/internal/cli"
	"github.com/mirzomumin/tulpar/internal/supervisor"
	"github.com/mirzomumin/tulpar/internal/worker"
)

func init() {
	// A worker runs the Python interpreter on the main goroutine, which has
	// to stay on the process main thread: CPython ties signal handling and
	// a few other things to it.
	runtime.LockOSThread()
}

func main() {
	if os.Getenv(supervisor.EnvWorker) != "" {
		// keep the marker away from anything the application may spawn
		os.Unsetenv(supervisor.EnvWorker)
		os.Exit(worker.Run())
	}
	os.Exit(cli.Main(os.Args[1:]))
}
