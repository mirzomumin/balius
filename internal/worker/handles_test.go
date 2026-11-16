package worker

import (
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleTable(t *testing.T) {
	var tbl handleTable
	h1 := tbl.put("a")
	h2 := tbl.put("b")
	if h1 == 0 || h1 == h2 || tbl.get(h1) != "a" || tbl.get(h2) != "b" {
		t.Fatal("put/get mismatch")
	}
	if tbl.del(h1) != "a" || tbl.get(h1) != nil {
		t.Fatal("del did not remove the value")
	}
	h3 := tbl.put("c") // reuses the slot of h1
	if tbl.get(h1) != nil || tbl.get(h3) != "c" {
		t.Error("a stale handle must not resolve to the slot's new value")
	}
	if tbl.del(h1) != nil {
		t.Error("deleting a stale handle must be a no-op")
	}
}

// headReader decodes a serialized request head, mirroring gc_head_parse.
type headReader struct{ b []byte }

func (r *headReader) u32() uint32 {
	v := binary.NativeEndian.Uint32(r.b)
	r.b = r.b[4:]
	return v
}

func (r *headReader) str() string {
	n := r.u32()
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

func TestBuildHead(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/a%20b/c?x=1&y=%20", nil)
	r.Host = "example.com"
	r.Header.Add("X-Multi", "1")
	r.Header.Add("X-Multi", "2")
	r.Header.Set("Sec-WebSocket-Protocol", "p1, p2")
	ci := &connInfo{serverIP: "127.0.0.1", serverPort: 80, clientIP: "10.0.0.1", clientPort: 5555}

	bp := buildHead(r, ci, true)
	defer putHead(bp)
	h := &headReader{b: *bp}
	if h.b[0] != 1 || h.b[1] != 0 {
		t.Errorf("version/tls bytes: %v", h.b[:4])
	}
	h.b = h.b[4:]
	if h.u32() != 80 || h.u32() != 5555 {
		t.Error("ports")
	}
	got := []string{h.str(), h.str(), h.str(), h.str(), h.str(), h.str(), h.str()}
	want := []string{"127.0.0.1", "10.0.0.1", "GET", "/a b/c", "/a%20b/c", "x=1&y=%20", ""}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %q, want %q", got, want)
	}
	if n := h.u32(); n != 2 || h.str() != "p1" || h.str() != "p2" {
		t.Error("subprotocols")
	}
	headers := map[string][]string{}
	for n := h.u32(); n > 0; n-- {
		k := h.str()
		headers[k] = append(headers[k], h.str())
	}
	if headers["host"][0] != "example.com" || strings.Join(headers["x-multi"], ",") != "1,2" {
		t.Errorf("headers: %v", headers)
	}
}

func TestBuildWSGIHead(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/p", strings.NewReader("body"))
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Add("Cookie", "a=1")
	r.Header.Add("Cookie", "b=2")
	r.Header.Add("X-Dashed-Name", "v")
	bp := buildWSGIHead(r, unknownConn)
	defer putHead(bp)
	h := &headReader{b: (*bp)[12:]}
	for i := 0; i < 7; i++ {
		h.str()
	}
	h.u32() // subprotocols
	env := map[string]string{}
	for n := h.u32(); n > 0; n-- {
		k := h.str()
		env[k] = h.str()
	}
	for k, want := range map[string]string{
		"CONTENT_TYPE": "text/plain", "HTTP_COOKIE": "a=1; b=2", "HTTP_X_DASHED_NAME": "v", "HTTP_HOST": "example.com",
	} {
		if env[k] != want {
			t.Errorf("%s: got %q, want %q", k, env[k], want)
		}
	}
}

func TestParseHeaders(t *testing.T) {
	var b []byte
	for _, s := range []string{"content-type", "text/plain", "x-a", ""} {
		b = binary.NativeEndian.AppendUint32(b, uint32(len(s)))
		b = append(b, s...)
	}
	got := parseHeaders(unsafePointer(b), len(b), 2)
	if len(got) != 2 || got[0] != (headerKV{"content-type", "text/plain"}) || got[1] != (headerKV{"x-a", ""}) {
		t.Errorf("got %v", got)
	}
	// truncated input must not panic
	parseHeaders(unsafePointer(b), len(b)-3, 2)
}
