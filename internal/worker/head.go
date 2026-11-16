package worker

// #include "glue.h"
import "C"

import (
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/mirzomumin/tulpar/internal/static"
)

// The request head crosses the Go/C boundary as a single flat buffer, so
// that building a scope costs one cgo call no matter how many headers the
// request carries. The layout is mirrored by gc_head_parse in glue.c:
//
//	u8  http version (0: 1.0, 1: 1.1, 2: 2)
//	u8  tls
//	u8  flags
//	u8  reserved
//	u32 server port
//	u32 client port
//	str server ip, client ip, method, path (percent-decoded), raw path,
//	    query string, authority
//	u32 count, then that many str: websocket subprotocols
//	u32 count, then that many (str, str): headers
//
// where str is a u32 length followed by the bytes; integers are host endian.

// connInfo is the per-connection part of the request head, computed once
// when the connection is accepted.
type connInfo struct {
	serverIP, clientIP     string
	serverPort, clientPort uint32
	uds                    bool
}

type connInfoKey struct{}

func newConnInfo(c net.Conn) *connInfo {
	ci := &connInfo{}
	switch a := c.LocalAddr().(type) {
	case *net.TCPAddr:
		ci.serverIP, ci.serverPort = a.IP.String(), uint32(a.Port)
	case *net.UnixAddr:
		ci.serverIP, ci.uds = a.Name, true
	}
	switch a := c.RemoteAddr().(type) {
	case *net.TCPAddr:
		ci.clientIP, ci.clientPort = a.IP.String(), uint32(a.Port)
	case *net.UnixAddr:
		ci.clientIP = a.Name
		if ci.clientIP == "@" {
			ci.clientIP = ""
		}
	}
	return ci
}

func connContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, connInfoKey{}, newConnInfo(c))
}

var unknownConn = &connInfo{}

func connInfoOf(r *http.Request) *connInfo {
	if ci, ok := r.Context().Value(connInfoKey{}).(*connInfo); ok {
		return ci
	}
	return unknownConn
}

var headPool = sync.Pool{New: func() any {
	b := make([]byte, 0, 2048)
	return &b
}}

func putStr(b []byte, s string) []byte {
	b = binary.NativeEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

// putLower appends s lower-cased (ASCII), without allocating.
func putLower(b []byte, s string) []byte {
	b = binary.NativeEndian.AppendUint32(b, uint32(len(s)))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b = append(b, c)
	}
	return b
}

// requestTarget splits the request target into its raw path and query.
func requestTarget(r *http.Request) (rawPath, query string) {
	uri := r.RequestURI
	if uri == "" || uri[0] != '/' {
		// absolute-form or authority-form targets
		return r.URL.EscapedPath(), r.URL.RawQuery
	}
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		return uri[:i], uri[i+1:]
	}
	return uri, ""
}

func headPrefix(b []byte, r *http.Request, ci *connInfo) []byte {
	var version byte
	switch {
	case r.ProtoMajor >= 2:
		version = 2
	case r.ProtoMinor >= 1:
		version = 1
	}
	var tls, flags byte
	if r.TLS != nil {
		tls = 1
	}
	authority := ""
	if r.ProtoMajor >= 2 {
		authority, flags = r.Host, flags|C.GC_HEAD_AUTHORITY
	} else if r.URL.Host != "" {
		authority, flags = r.URL.Host, flags|C.GC_HEAD_AUTHORITY
	}
	if ci.uds {
		flags |= C.GC_HEAD_UDS
	}
	rawPath, query := requestTarget(r)

	b = append(b, version, tls, flags, 0)
	b = binary.NativeEndian.AppendUint32(b, ci.serverPort)
	b = binary.NativeEndian.AppendUint32(b, ci.clientPort)
	b = putStr(b, ci.serverIP)
	b = putStr(b, ci.clientIP)
	b = putStr(b, r.Method)
	b = putStr(b, static.PercentDecode(rawPath))
	b = putStr(b, rawPath)
	b = putStr(b, query)
	b = putStr(b, authority)
	return b
}

// buildHead serializes a request for the ASGI and RSGI scope builders.
func buildHead(r *http.Request, ci *connInfo, websocket bool) *[]byte {
	bp := headPool.Get().(*[]byte)
	b := headPrefix((*bp)[:0], r, ci)

	// websocket subprotocols
	countAt := len(b)
	b = append(b, 0, 0, 0, 0)
	if websocket {
		var n uint32
		for _, v := range r.Header["Sec-Websocket-Protocol"] {
			for item := range strings.SplitSeq(v, ",") {
				if item = strings.TrimSpace(item); item != "" {
					b = putStr(b, item)
					n++
				}
			}
		}
		binary.NativeEndian.PutUint32(b[countAt:], n)
	}

	// headers: net/http keeps Host and Transfer-Encoding out of the map
	countAt = len(b)
	b = append(b, 0, 0, 0, 0)
	var n uint32
	if r.Host != "" {
		b = putStr(b, "host")
		b = putStr(b, r.Host)
		n++
	}
	for k, vs := range r.Header {
		for _, v := range vs {
			b = putLower(b, k)
			b = putStr(b, v)
			n++
		}
	}
	for _, te := range r.TransferEncoding {
		b = putStr(b, "transfer-encoding")
		b = putStr(b, te)
		n++
	}
	binary.NativeEndian.PutUint32(b[countAt:], n)

	*bp = b
	return bp
}

// putCGIName appends an HTTP header name as a CGI variable (`HTTP_X_FOO`).
func putCGIName(b []byte, name string) []byte {
	b = binary.NativeEndian.AppendUint32(b, uint32(len(name)+5))
	b = append(b, "HTTP_"...)
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '-':
			c = '_'
		case c >= 'a' && c <= 'z':
			c -= 'a' - 'A'
		}
		b = append(b, c)
	}
	return b
}

// buildWSGIHead serializes a request for the WSGI environ builder: the
// header section carries the final CGI variables.
func buildWSGIHead(r *http.Request, ci *connInfo) *[]byte {
	bp := headPool.Get().(*[]byte)
	b := headPrefix((*bp)[:0], r, ci)
	b = append(b, 0, 0, 0, 0) // no subprotocols

	countAt := len(b)
	b = append(b, 0, 0, 0, 0)
	var n uint32
	for k, vs := range r.Header {
		if len(vs) == 0 {
			continue
		}
		switch k {
		case "Content-Type":
			b = putStr(b, "CONTENT_TYPE")
			b = putStr(b, vs[0])
		case "Content-Length":
			b = putStr(b, "CONTENT_LENGTH")
			b = putStr(b, vs[0])
		case "Cookie":
			// cookies can't be joined by commas
			b = putCGIName(b, k)
			b = putStr(b, strings.Join(vs, "; "))
		default:
			b = putCGIName(b, k)
			if len(vs) == 1 {
				b = putStr(b, vs[0])
			} else {
				b = putStr(b, strings.Join(vs, ","))
			}
		}
		n++
	}
	b = putStr(b, "HTTP_HOST")
	b = putStr(b, r.Host)
	n++
	if len(r.TransferEncoding) > 0 {
		b = putStr(b, "HTTP_TRANSFER_ENCODING")
		b = putStr(b, strings.Join(r.TransferEncoding, ","))
		n++
	}
	binary.NativeEndian.PutUint32(b[countAt:], n)

	*bp = b
	return bp
}

func putHead(bp *[]byte) {
	if cap(*bp) <= 64*1024 {
		headPool.Put(bp)
	}
}
