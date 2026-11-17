package tests

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func randomBody(n int) []byte {
	b := make([]byte, n)
	r := rand.NewChaCha8([32]byte{byte(n), byte(n >> 8), byte(n >> 16)})
	_, _ = r.Read(b)
	return b
}

// A body of known length reaches an ASGI application in events of up to
// 1 MiB; an unknown length as it arrives. Either way nothing is lost.
func TestASGIRequestBodies(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "tests.apps.asgi:app")
	h2 := startServer(t, "--interface", "asgi", "--http", "2", "tests.apps.asgi:app")

	for _, c := range []struct {
		size   int
		events string
	}{
		{1, "1"},
		{70_000, "70000"},
		{1 << 20, "1048576"},
		{2_621_440, "1048576,1048576,524288"},
	} {
		t.Run(strconv.Itoa(c.size), func(t *testing.T) {
			body := randomBody(c.size)
			res := do(t, http.MethodPost, s.URL("/body_events"), bytes.NewReader(body))
			expect(t, "events", res.Header.Get("X-Events"), c.events)
			expect(t, "body intact", res.Body == string(body), true)

			res = doWith(t, h2cClient, http.MethodPost, h2.URL("/body_events"), bytes.NewReader(body))
			expect(t, "proto", res.Proto, "HTTP/2.0")
			expect(t, "h2 events", res.Header.Get("X-Events"), c.events)
			expect(t, "h2 body intact", res.Body == string(body), true)
		})
	}

	t.Run("unknown length", func(t *testing.T) {
		body := randomBody(1_500_000)
		// hiding the length makes the client send the body chunked
		res := do(t, http.MethodPost, s.URL("/body_events"), struct{ io.Reader }{bytes.NewReader(body)})
		expect(t, "body intact", res.Body == string(body), true)
		for _, e := range strings.Split(res.Header.Get("X-Events"), ",") {
			if n, _ := strconv.Atoi(e); n > 128<<10 {
				t.Errorf("event of %d bytes for a body of unknown length", n)
			}
		}
	})

	t.Run("client leaves mid-body", func(t *testing.T) {
		conn := dial(t, s)
		fmt.Fprintf(conn, "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", 2<<20)
		_, _ = conn.Write(randomBody(100_000))
		time.Sleep(100 * time.Millisecond)
		conn.Close()
		expect(t, "still serving", get(t, s.URL("/info")).Status, 200)
	})
}

// Response bodies held by the server (large bytes objects) must survive
// whatever the application does next, and must not leak.
func TestASGIResponseBodiesHeld(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "--workers-max-requests", "100000", "tests.apps.asgi:app")
	body := randomBody(300_000)
	for range 50 {
		res := post(t, s.URL("/echo"), string(body))
		if res.Body != string(body) {
			t.Fatal("echoed body differs")
		}
	}
	// clients leaving before the response is written
	for range 20 {
		conn := dial(t, s)
		fmt.Fprintf(conn, "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", len(body))
		_, _ = conn.Write(body)
		conn.Close()
	}
	expect(t, "still serving", get(t, s.URL("/info")).Status, 200)
}

// RSGI reads a body of known length straight into the bytes object
// `await protocol()` returns, and writes large bytes and str responses
// from where they are.
func TestRSGIBodies(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "tests.apps.rsgi:app")
	for _, size := range []int{1, 20_000, 300_000, 2_621_440} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			body := randomBody(size)
			res := do(t, http.MethodPost, s.URL("/echo"), bytes.NewReader(body))
			expect(t, "bytes body intact", res.Body == string(body), true)

			text := strings.Repeat("é-text ", size/8+1)
			expect(t, "str body intact", post(t, s.URL("/echo_str"), text).Body == text, true)

			res = do(t, http.MethodPost, s.URL("/echo"), struct{ io.Reader }{bytes.NewReader(body)})
			expect(t, "unknown length", res.Body == string(body), true)
		})
	}
	t.Run("clients leaving early", func(t *testing.T) {
		body := randomBody(300_000)
		for range 20 {
			conn := dial(t, s)
			fmt.Fprintf(conn, "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", 2*len(body))
			_, _ = conn.Write(body) // half of it
			conn.Close()

			conn = dial(t, s)
			fmt.Fprintf(conn, "POST /echo HTTP/1.1\r\nHost: localhost\r\nContent-Length: %d\r\n\r\n", len(body))
			_, _ = conn.Write(body) // all of it, but no waiting for the answer
			conn.Close()
		}
		expect(t, "still serving", get(t, s.URL("/info")).Status, 200)
	})
}
