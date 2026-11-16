package tests

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ---- websockets ----------------------------------------------------------

func dialWS(t *testing.T, url string, subprotocols ...string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: subprotocols,
		HTTPClient: &http.Client{Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}},
	})
}

func wsURL(s *server, path string) string {
	return fmt.Sprintf("ws://localhost:%d%s", s.Port, path)
}

func readWS(t *testing.T, c *websocket.Conn) (websocket.MessageType, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kind, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("websocket read: %v", err)
	}
	return kind, string(data)
}

func writeWS(t *testing.T, c *websocket.Conn, kind websocket.MessageType, data string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Write(ctx, kind, []byte(data)); err != nil {
		t.Fatalf("websocket write: %v", err)
	}
}

func TestWebsockets(t *testing.T) {
	for _, iface := range []string{"asgi", "rsgi"} {
		t.Run(iface, func(t *testing.T) {
			s := startServer(t, "--interface", iface, "tests.apps."+iface+":app")

			t.Run("messages", func(t *testing.T) {
				c, _, err := dialWS(t, wsURL(s, "/ws_echo"))
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseNow()
				writeWS(t, c, websocket.MessageText, "foo")
				kind, data := readWS(t, c)
				expect(t, "text", fmt.Sprint(kind, data), fmt.Sprint(websocket.MessageText, "foo"))
				writeWS(t, c, websocket.MessageBinary, "foo")
				kind, data = readWS(t, c)
				expect(t, "bytes", fmt.Sprint(kind, data), fmt.Sprint(websocket.MessageBinary, "foo"))
			})
			t.Run("reject", func(t *testing.T) {
				_, res, err := dialWS(t, wsURL(s, "/ws_reject"))
				if err == nil || res == nil {
					t.Fatalf("handshake should fail, got %v", err)
				}
				expect(t, "status", res.StatusCode, 403)
			})
			t.Run("scope", func(t *testing.T) {
				c, _, err := dialWS(t, wsURL(s, "/ws_info?test=true"), "proto1", "proto2")
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseNow()
				_, msg := readWS(t, c)
				var data map[string]any
				if err := json.Unmarshal([]byte(msg), &data); err != nil {
					t.Fatal(err)
				}
				expect(t, "http_version", data["http_version"], any("1.1"))
				expect(t, "path", data["path"], any("/ws_info"))
				expect(t, "query_string", data["query_string"], any("test=true"))
				expect(t, "host", data["headers"].(map[string]any)["host"], any(fmt.Sprintf("localhost:%d", s.Port)))
				if iface == "asgi" {
					expect(t, "type", data["type"], any("websocket"))
					expect(t, "scheme", data["scheme"], any("ws"))
					expect(t, "asgi", fmt.Sprint(data["asgi"]), "map[spec_version:2.3 version:3.0]")
					expect(t, "subprotocols", fmt.Sprint(data["subprotocols"]), "[proto1 proto2]")
					if _, ok := data["extensions"].(map[string]any)["websocket.http.response"]; !ok {
						t.Error("missing websocket.http.response extension")
					}
				} else {
					expect(t, "proto", data["proto"], any("ws"))
					expect(t, "rsgi_version", data["rsgi_version"], any("1.6"))
					expect(t, "scheme", data["scheme"], any("http"))
					expect(t, "method", data["method"], any("GET"))
					expect(t, "authority", data["authority"], nil)
				}
			})
			t.Run("push then client close", func(t *testing.T) {
				c, _, err := dialWS(t, wsURL(s, "/ws_push"))
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 50; i++ {
					readWS(t, c)
				}
				// the server must answer the close handshake although the
				// application never receives: Close fails without the echo
				if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
					t.Errorf("close handshake: %v", err)
				}
			})
			expect(t, "exit code", s.Stop(), 0)
		})
	}
}

func TestASGIWebsocketExtras(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "tests.apps.asgi:app")

	t.Run("reject explicit", func(t *testing.T) {
		_, res, err := dialWS(t, wsURL(s, "/ws_rejecte"))
		if err == nil || res == nil {
			t.Fatalf("handshake should fail, got %v", err)
		}
		expect(t, "status", res.StatusCode, 403)
	})
	t.Run("reject custom", func(t *testing.T) {
		_, res, err := dialWS(t, wsURL(s, "/ws_rejectc"))
		if err == nil || res == nil {
			t.Fatalf("handshake should fail, got %v", err)
		}
		body, _ := io.ReadAll(res.Body)
		expect(t, "status", res.StatusCode, 403)
		expect(t, "body", string(body), "WebSocket connection denied by application")
	})
	t.Run("server close", func(t *testing.T) {
		target := filepath.Join(t.TempDir(), "ws_result")
		c, _, err := dialWS(t, wsURL(s, "/ws_close"))
		if err != nil {
			t.Fatal(err)
		}
		writeWS(t, c, websocket.MessageText, target)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, err = c.Read(ctx)
		cancel()
		if websocket.CloseStatus(err) != websocket.StatusNormalClosure {
			t.Errorf("expected a normal close, got %v", err)
		}
		c.CloseNow()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(target); err == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Error("the application did not observe the disconnection")
	})
}

func TestWebsocketBadHandshake(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "tests.apps.asgi:app")
	res := get(t, s.URL("/ws_echo"), "Connection", "Upgrade", "Upgrade", "websocket")
	expect(t, "status", res.Status, 400)
}

// ---- TLS -----------------------------------------------------------------

func TestTLS(t *testing.T) {
	for _, iface := range []string{"asgi", "rsgi", "wsgi"} {
		for _, proto := range []struct {
			flag string
			max  uint16
		}{{"tls1.2", tls.VersionTLS12}, {"tls1.3", tls.VersionTLS13}} {
			t.Run(iface+" "+proto.flag, func(t *testing.T) {
				s := startServer(t, "--interface", iface, "--no-ws",
					"--ssl-certificate", fixture("tls", "cert.pem"), "--ssl-keyfile", fixture("tls", "key.pem"),
					"--ssl-protocol-min", proto.flag, "tests.apps."+iface+":app")
				conn, err := tls.Dial("tcp", fmt.Sprintf("localhost:%d", s.Port),
					&tls.Config{InsecureSkipVerify: true, MaxVersion: proto.max})
				if err != nil {
					t.Fatal(err)
				}
				expect(t, "version", conn.ConnectionState().Version, proto.max)
				conn.Close()

				res := get(t, s.HTTPSURL("/info?test=true"))
				expect(t, "status", res.Status, 200)
				expect(t, "scheme", decode(t, res.Body)["scheme"], any("https"))
				expect(t, "negotiated proto", res.Proto, "HTTP/2.0")
			})
		}
	}
}

func TestTLSMinVersionEnforced(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "--ssl-certificate", fixture("tls", "cert.pem"),
		"--ssl-keyfile", fixture("tls", "key.pem"), "tests.apps.rsgi:app")
	_, err := tls.Dial("tcp", fmt.Sprintf("localhost:%d", s.Port),
		&tls.Config{InsecureSkipVerify: true, MaxVersion: tls.VersionTLS12})
	if err == nil {
		t.Error("a TLS 1.2 handshake should be refused when the minimum is TLS 1.3")
	}
}

func TestTLSEncryptedKey(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "--ssl-certificate", fixture("tls", "pcert.pem"),
		"--ssl-keyfile", fixture("tls", "pkey.pem"), "--ssl-keyfile-password", "foobar", "tests.apps.rsgi:app")
	res := get(t, s.HTTPSURL("/info"))
	expect(t, "status", res.Status, 200)
	expect(t, "scheme", decode(t, res.Body)["scheme"], any("https"))
}

func TestTLSWebsocketScope(t *testing.T) {
	for iface, scheme := range map[string]string{"asgi": "wss", "rsgi": "https"} {
		t.Run(iface, func(t *testing.T) {
			s := startServer(t, "--interface", iface, "--ssl-certificate", fixture("tls", "cert.pem"),
				"--ssl-keyfile", fixture("tls", "key.pem"), "tests.apps."+iface+":app")
			c, _, err := dialWS(t, fmt.Sprintf("wss://localhost:%d/ws_info?test=true", s.Port))
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseNow()
			_, msg := readWS(t, c)
			var data map[string]any
			_ = json.Unmarshal([]byte(msg), &data)
			expect(t, "scheme", data["scheme"], any(scheme))
		})
	}
}

// ---- static files --------------------------------------------------------

func TestStaticFiles(t *testing.T) {
	for _, iface := range []string{"asgi", "rsgi", "wsgi"} {
		t.Run(iface, func(t *testing.T) {
			s := startServer(t, "--interface", iface, "--no-ws", "--static-path-mount", fixture("static"),
				"tests.apps."+iface+":app")
			for _, name := range []string{"media.png", "%E3%81%93%E3%82%93%E3%81%AB%E3%81%A1%E3%81%AF.png"} {
				res := get(t, s.URL("/static/"+name))
				expect(t, name+" status", res.Status, 200)
				expect(t, name+" content-type", res.Header.Get("Content-Type"), "image/png")
				if res.Header.Get("Cache-Control") == "" {
					t.Errorf("%s: missing cache-control", name)
				}
			}
			expect(t, "missing", get(t, s.URL("/static/missing.png")).Status, 404)
			expect(t, "dir without rewrite", get(t, s.URL("/static/empty")).Status, 404)
			expect(t, "app route", get(t, s.URL("/info")).Status, 200)
			for _, path := range []string{"/static/../conftest.py", "/static/%2e%2e/apps/asgi.py", "/static/..%2f..%2fgo.mod"} {
				expect(t, "traversal "+path, rawGet(t, s.Port, path), 404)
			}
		})
	}
}

// rawGet sends a request target verbatim (net/http would clean it).
func rawGet(t *testing.T, port int, target string) int {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n", target)
	var status int
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fscanf(conn, "HTTP/1.1 %d", &status); err != nil {
		t.Fatalf("reading status: %v", err)
	}
	return status
}

func TestStaticFilesRewriteAndMulti(t *testing.T) {
	s := startServer(t, "--interface", "rsgi",
		"--static-path-route", "/static", "--static-path-mount", fixture("static"),
		"--static-path-route", "/other", "--static-path-mount", fixture("tls"),
		"--static-path-dir-to-file", "index.txt", "tests.apps.rsgi:app")
	expect(t, "rewrite", get(t, s.URL("/static/file_rewrite")).Status, 200)
	expect(t, "rewrite missing", get(t, s.URL("/static/empty")).Status, 404)
	res := get(t, s.URL("/other/cert.pem"))
	expect(t, "second mount", res.Status, 200)
	expect(t, "pem type", res.Header.Get("Content-Type"), "application/x-x509-ca-cert")
}

// ---- sockets and workers -------------------------------------------------

func TestUnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "tulpar-uds")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	for _, iface := range []string{"asgi", "rsgi", "wsgi"} {
		t.Run(iface, func(t *testing.T) {
			path := filepath.Join(dir, iface+".sock")
			s := startServer(t, "--interface", iface, "--workers", "2", "--uds", path,
				"--uds-permissions", "660", "tests.apps."+iface+":app")
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			expect(t, "permissions", st.Mode().Perm(), os.FileMode(0o660))
			c := &http.Client{Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", path)
				},
			}}
			for i := 0; i < 4; i++ {
				res, err := c.Post("http://localhost/echo", "text/plain", strings.NewReader("uds"))
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				expect(t, "body", string(body), "uds")
			}
			expect(t, "exit code", s.Stop(), 0)
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Error("the socket file should be removed on shutdown")
			}
		})
	}
}

func TestMultipleWorkersAndMetrics(t *testing.T) {
	port := freePort(t)
	s := startServer(t, "--interface", "asgi", "--workers", "2", "--metrics",
		"--metrics-port", fmt.Sprint(port), "--metrics-scrape-interval", "1", "tests.apps.asgi:app")
	for i := 0; i < 20; i++ {
		expect(t, "status", get(t, s.URL("/info")).Status, 200)
	}
	time.Sleep(1500 * time.Millisecond)
	res := get(t, fmt.Sprintf("http://127.0.0.1:%d/", port))
	for _, want := range []string{
		"tulpar_workers_spawns 2", `tulpar_requests_handled{worker="1"}`, `tulpar_requests_handled{worker="2"}`,
		"# TYPE tulpar_connections_active gauge",
	} {
		if !strings.Contains(res.Body, want) {
			t.Errorf("metrics should contain %q:\n%s", want, res.Body)
		}
	}
}

func TestWorkerRespawn(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "--respawn-failed-workers", "tests.apps.rsgi:app")
	pid := workerPIDs(t, s.cmd.Process.Pid)
	if len(pid) != 1 {
		t.Fatalf("expected one worker, got %v", pid)
	}
	p, _ := os.FindProcess(pid[0])
	_ = p.Kill()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Count(s.Output(), "Started worker-1") == 2 {
			expect(t, "status", get(t, s.URL("/info")).Status, 200)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("worker was not respawned:\n%s", s.Output())
}

func TestWorkerFailureStopsServer(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "tests.apps.rsgi:app")
	for _, pid := range workerPIDs(t, s.cmd.Process.Pid) {
		p, _ := os.FindProcess(pid)
		_ = p.Kill()
	}
	select {
	case <-s.exited:
		expect(t, "exit code", s.cmd.ProcessState.ExitCode(), 1)
	case <-time.After(10 * time.Second):
		t.Fatal("the server should exit when a worker dies")
	}
}

func TestBadTarget(t *testing.T) {
	cmd := cliCmd(t, "--interface", "asgi", "--port", "0", "tests.apps.missing:app")
	out, err := cmd.CombinedOutput()
	if err == nil || cmd.ProcessState.ExitCode() != 1 {
		t.Fatalf("expected exit code 1, got %v", err)
	}
	if !strings.Contains(string(out), "Could not import 'tests.apps.missing'") {
		t.Errorf("unexpected output:\n%s", out)
	}
}

func TestCLIErrors(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"--interface", "nope", "app:app"},
		{"--workers", "0", "app:app"},
		{"--interface", "asgi", "--blocking-threads", "2", "app:app"},
		{"--no-such-flag", "app:app"},
	} {
		cmd := cliCmd(t, args...)
		out, _ := cmd.CombinedOutput()
		if cmd.ProcessState.ExitCode() != 2 {
			t.Errorf("%v: expected exit code 2, got %d:\n%s", args, cmd.ProcessState.ExitCode(), out)
		}
	}
}

func cliCmd(t *testing.T, args ...string) *exec.Cmd {
	cmd := exec.Command(binary(t), args...)
	cmd.Dir = repoRoot
	return cmd
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// workerPIDs lists the children of the supervisor. /proc/PID/task/PID/children
// only covers children forked by the main thread: it works because the
// supervisor keeps its main goroutine locked there (main.go).
func workerPIDs(t *testing.T, parent int) []int {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", parent, parent))
	if err != nil {
		t.Skipf("cannot list child processes: %v", err)
	}
	var pids []int
	for _, f := range strings.Fields(string(data)) {
		var pid int
		fmt.Sscan(f, &pid)
		pids = append(pids, pid)
	}
	return pids
}
