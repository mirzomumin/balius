// Package tests holds the end-to-end tests of tulpar: they run the real
// binary against the Python applications in tests/apps, mirroring granian's
// test suite.
//
// Run them through `make test-integration`, or point TULPAR_BIN at a built
// binary and run `go test ./tests/`.
package tests

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// repoRoot is the directory holding go.mod: the apps are imported as
// `tests.apps.<name>` from there.
var repoRoot = func() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(file))
}()

func fixture(parts ...string) string {
	return filepath.Join(append([]string{repoRoot, "tests", "fixtures"}, parts...)...)
}

func binary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("TULPAR_BIN")
	if bin == "" {
		bin = filepath.Join(repoRoot, "bin", "tulpar")
	}
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("tulpar binary not found at %s (build it with `make build` or set TULPAR_BIN)", bin)
	}
	return bin
}

// server is a running tulpar instance.
type server struct {
	t    *testing.T
	cmd  *exec.Cmd
	Port int
	// Addr is host:port, or the socket path for unix sockets.
	Addr string

	mu     sync.Mutex
	output bytes.Buffer
	exited chan struct{}
}

var listenRe = regexp.MustCompile(`Listening at: \w+://(\S+)`)

// startServer runs tulpar with the given arguments (the target included)
// and waits until every worker is serving. Unless the arguments say
// otherwise it binds 127.0.0.1 on a free port.
func startServer(t *testing.T, args ...string) *server {
	t.Helper()
	full := args
	if !hasArg(args, "--uds") && !hasArg(args, "--port") {
		full = append([]string{"--port", "0"}, args...)
	}
	cmd := exec.Command(binary(t), full...)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "PYTHONDONTWRITEBYTECODE=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &server{t: t, cmd: cmd, exited: make(chan struct{})}

	workers := 1
	if v := argValue(args, "--workers"); v != "" {
		workers, _ = strconv.Atoi(v)
	}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		started, addr := 0, ""
		for scanner.Scan() {
			line := scanner.Text()
			s.mu.Lock()
			s.output.WriteString(line + "\n")
			s.mu.Unlock()
			if m := listenRe.FindStringSubmatch(line); m != nil {
				addr = m[1]
			}
			if strings.Contains(line, "] Started worker-") {
				if started++; started == workers {
					ready <- addr
				}
			}
		}
	}()
	go func() {
		_ = cmd.Wait()
		close(s.exited)
	}()
	t.Cleanup(s.stop)

	select {
	case addr := <-ready:
		if path, ok := strings.CutPrefix(addr, "unix:"); ok {
			s.Addr = path
		} else {
			s.Addr = addr
			_, port, _ := net.SplitHostPort(addr)
			s.Port, _ = strconv.Atoi(port)
		}
	case <-s.exited:
		t.Fatalf("tulpar exited before serving:\n%s", s.Output())
	case <-time.After(20 * time.Second):
		t.Fatalf("tulpar did not start in time:\n%s", s.Output())
	}
	return s
}

func hasArg(args []string, name string) bool { return argValue(args, name) != "" }

func argValue(args []string, name string) string {
	for i, a := range args {
		if a == name && i+1 < len(args) {
			return args[i+1]
		}
		if v, ok := strings.CutPrefix(a, name+"="); ok {
			return v
		}
	}
	return ""
}

// Output returns everything the server logged so far.
func (s *server) Output() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.output.String()
}

// URL builds an http URL on the server, using `localhost` as the host
// (the tests check the Host header the application sees).
func (s *server) URL(path string) string {
	return fmt.Sprintf("http://localhost:%d%s", s.Port, path)
}

func (s *server) HTTPSURL(path string) string {
	return fmt.Sprintf("https://localhost:%d%s", s.Port, path)
}

// Stop terminates the server gracefully and returns its exit code.
func (s *server) Stop() int {
	_ = s.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-s.exited:
	case <-time.After(15 * time.Second):
		_ = s.cmd.Process.Kill()
		<-s.exited
		s.t.Errorf("tulpar did not stop gracefully:\n%s", s.Output())
	}
	return s.cmd.ProcessState.ExitCode()
}

func (s *server) stop() {
	select {
	case <-s.exited:
	default:
		s.Stop()
	}
	if s.t.Failed() {
		s.t.Logf("server output:\n%s", s.Output())
	}
}

var client = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
		DisableKeepAlives: true,
		ForceAttemptHTTP2: true,
	},
}

type response struct {
	Status  int
	Header  http.Header
	Body    string
	Proto   string
	TLSVers uint16
}

func do(t *testing.T, method, url string, body io.Reader, headers ...string) response {
	t.Helper()
	return doWith(t, client, method, url, body, headers...)
}

// h2cClient speaks HTTP/2 without TLS (prior knowledge).
var h2cClient = func() *http.Client {
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	return &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{Protocols: protocols}}
}()

func doWith(t *testing.T, client *http.Client, method, url string, body io.Reader, headers ...string) response {
	t.Helper()
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Add(headers[i], headers[i+1])
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("%s %s: reading body: %v", method, url, err)
	}
	r := response{Status: res.StatusCode, Header: res.Header, Body: string(data), Proto: res.Proto}
	if res.TLS != nil {
		r.TLSVers = res.TLS.Version
	}
	return r
}

func get(t *testing.T, url string, headers ...string) response {
	t.Helper()
	return do(t, http.MethodGet, url, nil, headers...)
}

func post(t *testing.T, url, body string, headers ...string) response {
	t.Helper()
	return do(t, http.MethodPost, url, strings.NewReader(body), headers...)
}

func expect[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s: got %v, want %v", what, got, want)
	}
}

// largeBody is the payload of granian's `test_body_large`.
func largeBody() string {
	var b strings.Builder
	for i := 0; i < 5000; i++ {
		s := fmt.Sprintf("%dtest", i)
		b.WriteString(strings.Repeat("0", max(0, 8-len(s))) + s)
	}
	return b.String()
}
