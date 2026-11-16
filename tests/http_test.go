package tests

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func decode(t *testing.T, body string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("invalid JSON %q: %v", body, err)
	}
	return out
}

// ---- ASGI ----------------------------------------------------------------

func TestASGI(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "--no-ws", "tests.apps.asgi:app")

	t.Run("scope", func(t *testing.T) {
		res := get(t, s.URL("/info?test=true"))
		expect(t, "status", res.Status, 200)
		expect(t, "content-type", res.Header.Get("Content-Type"), "application/json")
		expect(t, "server", res.Header.Get("Server"), "balius")
		data := decode(t, res.Body)
		expect(t, "asgi", fmt.Sprint(data["asgi"]), "map[spec_version:2.3 version:3.0]")
		expect(t, "type", data["type"], any("http"))
		expect(t, "http_version", data["http_version"], any("1.1"))
		expect(t, "scheme", data["scheme"], any("http"))
		expect(t, "method", data["method"], any("GET"))
		expect(t, "path", data["path"], any("/info"))
		expect(t, "query_string", data["query_string"], any("test=true"))
		expect(t, "host", data["headers"].(map[string]any)["host"], any(fmt.Sprintf("localhost:%d", s.Port)))
		expect(t, "state", fmt.Sprint(data["state"]), "map[global:test]")
		if _, ok := data["extensions"].(map[string]any)["http.response.pathsend"]; !ok {
			t.Error("missing pathsend extension")
		}
	})
	t.Run("path utf8 lossy", func(t *testing.T) {
		res := get(t, s.URL("/%c0"))
		expect(t, "status", res.Status, 200)
		expect(t, "path", decode(t, res.Body)["path"], any("/�"))
	})
	t.Run("body", func(t *testing.T) {
		res := post(t, s.URL("/echo"), "test")
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, "test")
	})
	t.Run("body large", func(t *testing.T) {
		data := largeBody()
		res := post(t, s.URL("/echo"), data)
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, data)
	})
	t.Run("app error", func(t *testing.T) {
		expect(t, "status", get(t, s.URL("/err_app")).Status, 500)
	})
	t.Run("protocol error", func(t *testing.T) {
		res := get(t, s.URL("/err_proto/type"))
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, "Unsupported ASGI message")
		res = get(t, s.URL("/err_proto/flow"))
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, "msg1")
	})
	t.Run("pathsend", func(t *testing.T) {
		res := get(t, s.URL("/file"))
		expect(t, "status", res.Status, 200)
		expect(t, "content-type", res.Header.Get("Content-Type"), "image/png")
		expect(t, "content-length", res.Header.Get("Content-Length"), "95")
		expect(t, "length", len(res.Body), 95)
	})
	t.Run("sniffio", func(t *testing.T) {
		res := get(t, s.URL("/sniffio"))
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, "asyncio")
	})
	t.Run("scope is per request", func(t *testing.T) {
		for range 3 {
			expect(t, "extensions", get(t, s.URL("/mutate")).Body, "1")
		}
	})
	t.Run("timeout", func(t *testing.T) {
		expect(t, "no timeout", get(t, s.URL("/timeout_n")).Body, "ok")
		expect(t, "timeout", get(t, s.URL("/timeout_w")).Body, "timeout")
	})
	expect(t, "exit code", s.Stop(), 0)
}

func TestASGINoLifespan(t *testing.T) {
	s := startServer(t, "--interface", "asginl", "tests.apps.asgi:app")
	res := get(t, s.URL("/info"))
	expect(t, "status", res.Status, 200)
	// no lifespan: the state stays empty
	expect(t, "state", fmt.Sprint(decode(t, res.Body)["state"]), "map[]")
}

// ---- RSGI ----------------------------------------------------------------

const rangeFileContent = "0123456789"

func TestRSGI(t *testing.T) {
	s := startServer(t, "--interface", "rsgi", "--no-ws", "tests.apps.rsgi:app")

	t.Run("scope", func(t *testing.T) {
		res := get(t, s.URL("/info?test=true"))
		expect(t, "status", res.Status, 200)
		expect(t, "content-type", res.Header.Get("Content-Type"), "application/json")
		data := decode(t, res.Body)
		expect(t, "proto", data["proto"], any("http"))
		expect(t, "http_version", data["http_version"], any("1.1"))
		expect(t, "rsgi_version", data["rsgi_version"], any("1.6"))
		expect(t, "scheme", data["scheme"], any("http"))
		expect(t, "method", data["method"], any("GET"))
		expect(t, "path", data["path"], any("/info"))
		expect(t, "query_string", data["query_string"], any("test=true"))
		expect(t, "host", data["headers"].(map[string]any)["host"], any(fmt.Sprintf("localhost:%d", s.Port)))
		expect(t, "authority", data["authority"], nil)
	})
	t.Run("body", func(t *testing.T) {
		expect(t, "body", post(t, s.URL("/echo"), "test").Body, "test")
	})
	t.Run("body large", func(t *testing.T) {
		data := largeBody()
		expect(t, "body", post(t, s.URL("/echo"), data).Body, data)
	})
	t.Run("body stream request", func(t *testing.T) {
		data := largeBody()
		res := post(t, s.URL("/echos"), data)
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, data)
	})
	t.Run("body stream response", func(t *testing.T) {
		res := get(t, s.URL("/stream"))
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, "testtesttest")
	})
	t.Run("file", func(t *testing.T) {
		res := get(t, s.URL("/file"))
		expect(t, "status", res.Status, 200)
		expect(t, "content-type", res.Header.Get("Content-Type"), "image/png")
		expect(t, "content-length", res.Header.Get("Content-Length"), "95")
	})

	file := filepath.Join(t.TempDir(), "temp_file.txt")
	if err := os.WriteFile(file, []byte(rangeFileContent), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, r := range [][2]int{{0, 10}, {0, 5}, {5, 10}, {3, 7}, {9, 10}} {
		start, end := r[0], r[1]
		t.Run(fmt.Sprintf("file range %d-%d", start, end), func(t *testing.T) {
			res := get(t, s.URL("/file_range"), "file-path", file, "range", fmt.Sprintf("bytes=%d-%d", start, end-1))
			expect(t, "status", res.Status, 206)
			expect(t, "content-range", res.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/10", start, end-1))
			expect(t, "content-length", res.Header.Get("Content-Length"), fmt.Sprint(end-start))
			expect(t, "body", res.Body, rangeFileContent[start:end])
		})
	}
	t.Run("file range exceeding", func(t *testing.T) {
		res := get(t, s.URL("/file_range"), "file-path", file, "range", "bytes=0-20")
		expect(t, "status", res.Status, 206)
		expect(t, "content-range", res.Header.Get("Content-Range"), "bytes 0-9/10")
		expect(t, "body", res.Body, rangeFileContent)
	})
	t.Run("file range unsatisfiable", func(t *testing.T) {
		res := get(t, s.URL("/file_range"), "file-path", file, "range", "bytes=10-20")
		expect(t, "status", res.Status, 416)
		expect(t, "content-range", res.Header.Get("Content-Range"), "bytes */10")
		expect(t, "reason", res.Header.Get("4xx-Reason"), "out")
		res = get(t, s.URL("/file_range"), "file-path", file, "range", "bytes=8-4")
		expect(t, "status", res.Status, 416)
		expect(t, "reason", res.Header.Get("4xx-Reason"), "invalid")
	})
	t.Run("app error", func(t *testing.T) {
		expect(t, "status", get(t, s.URL("/err_app")).Status, 500)
	})
	expect(t, "exit code", s.Stop(), 0)
}

// ---- WSGI ----------------------------------------------------------------

func TestWSGI(t *testing.T) {
	s := startServer(t, "--interface", "wsgi", "--blocking-threads", "4", "tests.apps.wsgi:app")

	t.Run("scope", func(t *testing.T) {
		payload := "body_payload"
		res := post(t, s.URL("/info?test=true"), payload, "test", "val1", "test", "val2")
		expect(t, "status", res.Status, 200)
		expect(t, "content-type", res.Header.Get("Content-Type"), "application/json")
		data := decode(t, res.Body)
		headers := data["headers"].(map[string]any)
		expect(t, "scheme", data["scheme"], any("http"))
		expect(t, "method", data["method"], any("POST"))
		expect(t, "path", data["path"], any("/info"))
		expect(t, "query_string", data["query_string"], any("test=true"))
		expect(t, "host", headers["HTTP_HOST"], any(fmt.Sprintf("localhost:%d", s.Port)))
		expect(t, "content_length", data["content_length"], any(fmt.Sprint(len(payload))))
		expect(t, "repeated header", headers["HTTP_TEST"], any("val1,val2"))
	})
	t.Run("body", func(t *testing.T) {
		expect(t, "body", post(t, s.URL("/echo"), "test").Body, "test")
	})
	t.Run("body large", func(t *testing.T) {
		data := largeBody()
		expect(t, "body", post(t, s.URL("/echo"), data).Body, data)
	})
	t.Run("iterbody", func(t *testing.T) {
		res := get(t, s.URL("/iterbody"))
		expect(t, "status", res.Status, 200)
		expect(t, "body", res.Body, "testtesttest")
	})
	t.Run("lazy start_response", func(t *testing.T) {
		res := get(t, s.URL("/lazy"))
		expect(t, "status", res.Status, 201)
		expect(t, "header", res.Header.Get("X-Gen"), "yes")
		expect(t, "body", res.Body, "lazy")
	})
	t.Run("failure mid-body is not a complete response", func(t *testing.T) {
		res, err := client.Get(s.URL("/err_iter"))
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if _, err := io.ReadAll(res.Body); err == nil {
			t.Error("the truncated body must not read as complete")
		}
	})
	t.Run("app error", func(t *testing.T) {
		expect(t, "status", get(t, s.URL("/err_app")).Status, 500)
	})
	expect(t, "exit code", s.Stop(), 0)
}

// ---- protocols -----------------------------------------------------------

func TestHTTP2Cleartext(t *testing.T) {
	for _, iface := range []string{"asgi", "rsgi", "wsgi"} {
		t.Run(iface, func(t *testing.T) {
			s := startServer(t, "--interface", iface, "--http", "2", "tests.apps."+iface+":app")
			res := doWith(t, h2cClient, http.MethodPost, s.URL("/echo"), strings.NewReader("h2 body"))
			expect(t, "proto", res.Proto, "HTTP/2.0")
			expect(t, "body", res.Body, "h2 body")
		})
	}
}

func TestURLPathPrefix(t *testing.T) {
	s := startServer(t, "--interface", "wsgi", "--url-path-prefix", "/api", "tests.apps.wsgi:app")
	res := get(t, s.URL("/api/info"))
	expect(t, "status", res.Status, 200)
	expect(t, "path", decode(t, res.Body)["path"], any("/info"))
}

func TestAccessLog(t *testing.T) {
	s := startServer(t, "--interface", "asgi", "--access-log",
		"--access-log-fmt", `%(method)s %(path)s %(status)d %(header{user-agent})s`, "tests.apps.asgi:app")
	get(t, s.URL("/info"), "User-Agent", "balius-test")
	s.Stop()
	if !strings.Contains(s.Output(), "GET /info 200 balius-test") {
		t.Errorf("access log line not found in:\n%s", s.Output())
	}
}
