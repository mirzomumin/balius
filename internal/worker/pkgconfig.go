//go:build !tulpar_pyflags

package worker

// By default the interpreter comes from the system's `python3-embed`
// pkg-config module. Building with the `tulpar_pyflags` tag disables this,
// and the flags are expected in CGO_CFLAGS / CGO_LDFLAGS instead (see the
// Makefile), which also lets the build cache tell interpreters apart.

/*
#cgo pkg-config: python3-embed
*/
import "C"
