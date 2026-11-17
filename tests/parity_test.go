package tests

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func dial(t *testing.T, s *server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// rawRequest sends a request verbatim and returns the response body (the
// request must ask for `Connection: close`, or be HTTP/1.0).
func rawRequest(t *testing.T, s *server, request string) string {
	t.Helper()
	conn := dial(t, s)
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	fmt.Fprint(conn, request)
	data, _ := io.ReadAll(conn)
	_, body, _ := strings.Cut(string(data), "\r\n\r\n")
	return body
}

// closedWithin reports whether the server closes conn within d.
func closedWithin(conn net.Conn, d time.Duration) bool {
	_ = conn.SetReadDeadline(time.Now().Add(d))
	_, err := io.Copy(io.Discard, conn)
	return err == nil // EOF: closed by the server; a timeout is an error
}

// ---- idle connections ------------------------------------------------------

func TestKeepAliveTimeout(t *testing.T) {
	// a single connection slot: an idle client must not hold it forever
	s := startServer(t, "--interface", "asgi", "--keep-alive-timeout", "1", "--backpressure", "1",
		"tests.apps.asgi:app")
	idle := dial(t, s)
	fmt.Fprint(idle, "GET /info HTTP/1.1\r\nHost: localhost\r\n\r\n")
	reader := bufio.NewReader(idle)
	if line, err := reader.ReadString('\n'); err != nil || !strings.Contains(line, "200") {
		t.Fatalf("first request: %q %v", line, err)
	}
	// drain the response, then expect the server to drop the idle connection
	if !closedWithin(idle, 5*time.Second) {
		t.Fatal("the idle keep-alive connection was not closed")
	}
	expect(t, "next client is served", get(t, s.URL("/info")).Status, 200)
}

// ---- HTTP/1.0 ----------------------------------------------------------------

func TestHTTP10Version(t *testing.T) {
	for iface, want := range map[string]string{"asgi": "1.0", "rsgi": "1"} {
		t.Run(iface, func(t *testing.T) {
			s := startServer(t, "--interface", iface, "tests.apps."+iface+":app")
			body := rawRequest(t, s, "GET /info HTTP/1.0\r\n\r\n")
			expect(t, "http_version", decode(t, body)["http_version"], any(want))
		})
	}
	t.Run("wsgi", func(t *testing.T) {
		s := startServer(t, "--interface", "wsgi", "tests.apps.wsgi:app")
		body := rawRequest(t, s, "GET /info HTTP/1.0\r\n\r\n")
		expect(t, "SERVER_PROTOCOL", decode(t, body)["protocol"], any("HTTP/1.0"))
	})
}

// ---- websockets ----------------------------------------------------------------

func TestWebsocketKeepalive(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "--ws-ping-interval", "1", "--ws-ping-timeout", "1",
		"tests.apps.asgi:app")

	t.Run("a responsive client stays connected", func(t *testing.T) {
		c, _, err := dialWS(t, wsURL(s, "/ws_echo"))
		if err != nil {
			t.Fatal(err)
		}
		defer c.CloseNow()
		// pongs are sent while reading: echo across several ping periods
		for range 4 {
			time.Sleep(800 * time.Millisecond)
			writeWS(t, c, websocket.MessageText, "still here")
			_, msg := readWS(t, c)
			expect(t, "echo", msg, "still here")
		}
	})
	t.Run("a silent client is dropped", func(t *testing.T) {
		conn := dial(t, s)
		fmt.Fprint(conn, "GET /ws_echo HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
		// never answers the pings
		if !closedWithin(conn, 12*time.Second) {
			t.Fatal("a client that doesn't answer pings was not disconnected")
		}
	})
}

func TestWebsocketMaxSize(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "--ws-max-size", "1000", "tests.apps.rsgi:app")
	c, _, err := dialWS(t, wsURL(s, "/ws_echo"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	writeWS(t, c, websocket.MessageText, "small")
	_, msg := readWS(t, c)
	expect(t, "small message", msg, "small")

	writeWS(t, c, websocket.MessageText, strings.Repeat("x", 2000))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, err = c.Read(ctx)
	expect(t, "close code", websocket.CloseStatus(err), websocket.StatusMessageTooBig)
}

func TestWebsocketCompression(t *testing.T) {
	for flag, want := range map[string]bool{"--ws-per-message-deflate": true, "--no-ws-per-message-deflate": false} {
		t.Run(flag, func(t *testing.T) {
			s := startServer(t, "--interface", "asgi", flag, "tests.apps.asgi:app")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, res, err := websocket.Dial(ctx, wsURL(s, "/ws_echo"),
				&websocket.DialOptions{CompressionMode: websocket.CompressionContextTakeover})
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseNow()
			negotiated := strings.Contains(res.Header.Get("Sec-WebSocket-Extensions"), "permessage-deflate")
			expect(t, "compression negotiated", negotiated, want)
			// large enough to be compressed
			payload := strings.Repeat("compress me ", 500)
			writeWS(t, c, websocket.MessageText, payload)
			_, msg := readWS(t, c)
			expect(t, "echo", msg, payload)
		})
	}
}

// ---- proxy headers -------------------------------------------------------------

func TestProxyHeaders(t *testing.T) {
	forwarded := []string{"X-Forwarded-For", "203.0.113.7, 127.0.0.1", "X-Forwarded-Proto", "https"}
	client := func(iface string, data map[string]any) string {
		switch iface {
		case "asgi":
			return fmt.Sprint(data["client"].([]any)[0])
		case "rsgi":
			host, _, _ := strings.Cut(data["client"].(string), ":")
			return host
		}
		return data["client"].(string)
	}
	for _, iface := range []string{"asgi", "rsgi", "wsgi"} {
		t.Run(iface+" trusted proxy", func(t *testing.T) {
			s := startServer(t, "--interface", iface, "tests.apps."+iface+":app")
			data := decode(t, get(t, s.URL("/info"), forwarded...).Body)
			expect(t, "client", client(iface, data), "203.0.113.7")
			expect(t, "scheme", data["scheme"], any("https"))

			data = decode(t, get(t, s.URL("/info")).Body)
			expect(t, "client without headers", client(iface, data), "127.0.0.1")
			expect(t, "scheme without headers", data["scheme"], any("http"))
		})
	}
	for name, args := range map[string][]string{
		"disabled":        {"--no-proxy-headers"},
		"untrusted proxy": {"--forwarded-allow-ips", "10.0.0.1,192.168.0.0/16"},
	} {
		t.Run(name, func(t *testing.T) {
			s := startServer(t, append([]string{"--interface", "asgi"}, append(args, "tests.apps.asgi:app")...)...)
			data := decode(t, get(t, s.URL("/info"), forwarded...).Body)
			expect(t, "client", client("asgi", data), "127.0.0.1")
			expect(t, "scheme", data["scheme"], any("http"))
		})
	}
	t.Run("websocket scheme", func(t *testing.T) {
		s := startServer(t, "--interface", "asgi", "tests.apps.asgi:app")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		c, _, err := websocket.Dial(ctx, wsURL(s, "/ws_info"), &websocket.DialOptions{
			HTTPHeader: map[string][]string{"X-Forwarded-Proto": {"https"}, "X-Forwarded-For": {"203.0.113.7"}},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.CloseNow()
		_, msg := readWS(t, c)
		var data map[string]any
		_ = json.Unmarshal([]byte(msg), &data)
		expect(t, "scheme", data["scheme"], any("wss"))
	})
}

// ---- worker recycling ----------------------------------------------------------

func TestWorkersMaxRequests(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "--workers-max-requests", "10", "--respawn-interval", "0.3",
		"tests.apps.rsgi:app")
	// requests keep being served while the worker is replaced
	for i := range 60 {
		if status := get(t, s.URL("/info")).Status; status != 200 {
			t.Fatalf("request %d: status %d", i, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if out := s.Output(); !strings.Contains(out, "served its share of requests") ||
		strings.Count(out, "Started worker-1") < 2 {
		t.Errorf("the worker was not recycled:\n%s", out)
	}
	expect(t, "exit code", s.Stop(), 0)
}

func TestWorkerHealthcheck(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "--workers-healthcheck-timeout", "2", "tests.apps.asgi:app")
	expect(t, "healthy", get(t, s.URL("/info")).Status, 200)

	// freeze the interpreter: this request never completes
	go func() {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", s.Port))
		if err == nil {
			fmt.Fprint(conn, "GET /hang HTTP/1.1\r\nHost: localhost\r\n\r\n")
			time.Sleep(20 * time.Second)
			conn.Close()
		}
	}()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if out := s.Output(); strings.Contains(out, "is unresponsive") && strings.Count(out, "Started worker-1") >= 2 {
			expect(t, "served by the replacement", get(t, s.URL("/info")).Status, 200)
			expect(t, "exit code", s.Stop(), 0)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("the frozen worker was not replaced:\n%s", s.Output())
}
