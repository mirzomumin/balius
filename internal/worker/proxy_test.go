package worker

import (
	"net/http"
	"testing"
)

func TestTrustedProxies(t *testing.T) {
	proxies := parseTrustedProxies("127.0.0.1, 10.0.0.0/8, ::1, /run/proxy.sock, ")
	for host, want := range map[string]bool{
		"127.0.0.1":       true,
		"10.20.30.40":     true,
		"::1":             true,
		"/run/proxy.sock": true,
		"11.0.0.1":        false,
		"192.168.1.1":     false,
		"somewhere":       false,
		"":                false,
	} {
		if got := proxies.trusts(host); got != want {
			t.Errorf("trusts(%q) = %v, want %v", host, got, want)
		}
	}
	if !parseTrustedProxies("*").trusts("") {
		t.Error("`*` must trust every peer, unix sockets included")
	}
}

func TestForwardedHostPort(t *testing.T) {
	for in, want := range map[string]struct {
		host string
		port uint32
	}{
		"203.0.113.7":        {"203.0.113.7", 0},
		"203.0.113.7:4711":   {"203.0.113.7", 4711},
		"2001:db8::1":        {"2001:db8::1", 0},
		"[2001:db8::1]":      {"2001:db8::1", 0},
		"[2001:db8::1]:4711": {"2001:db8::1", 4711},
		"[2001:db8::1]:nope": {"2001:db8::1", 0},
		"host:nope":          {"host:nope", 0},
		"host:":              {"host:", 0},
		"[broken":            {"[broken", 0},
		"unknown":            {"unknown", 0},
	} {
		if host, port := forwardedHostPort(in); host != want.host || port != want.port {
			t.Errorf("%q: got %q %d, want %q %d", in, host, port, want.host, want.port)
		}
	}
}

func TestProxyApply(t *testing.T) {
	proxies := parseTrustedProxies("127.0.0.1,10.0.0.0/8")
	for _, c := range []struct {
		name      string
		forwarded []string
		proto     []string
		tls       bool
		wantIP    string
		wantPort  uint32
		wantTLS   bool
	}{
		{name: "no headers", wantIP: "127.0.0.1", wantPort: 5000},
		{name: "single client", forwarded: []string{"203.0.113.7"}, proto: []string{"https"}, wantIP: "203.0.113.7", wantTLS: true},
		// the rightmost entry that isn't a trusted proxy: entries on its left may be forged
		{name: "proxy chain", forwarded: []string{"6.6.6.6, 203.0.113.7:1234, 10.0.0.2"}, wantIP: "203.0.113.7", wantPort: 1234},
		{name: "repeated headers", forwarded: []string{"6.6.6.6", "203.0.113.7"}, wantIP: "203.0.113.7"},
		{name: "only proxies", forwarded: []string{"10.0.0.3, 10.0.0.2"}, wantIP: "10.0.0.3"},
		{name: "downgrade", proto: []string{"http"}, tls: true, wantIP: "127.0.0.1", wantPort: 5000},
		{name: "websocket scheme", proto: []string{"wss"}, wantIP: "127.0.0.1", wantPort: 5000, wantTLS: true},
		{name: "unknown scheme", proto: []string{"gopher"}, tls: true, wantIP: "127.0.0.1", wantPort: 5000, wantTLS: true},
		{name: "last proto wins", proto: []string{"http", "https"}, wantIP: "127.0.0.1", wantPort: 5000, wantTLS: true},
	} {
		h := http.Header{}
		if c.forwarded != nil {
			h["X-Forwarded-For"] = c.forwarded
		}
		if c.proto != nil {
			h["X-Forwarded-Proto"] = c.proto
		}
		ip, port, tls := proxies.apply(h, "127.0.0.1", 5000, c.tls)
		if ip != c.wantIP || port != c.wantPort || tls != c.wantTLS {
			t.Errorf("%s: got %s %d %v, want %s %d %v", c.name, ip, port, tls, c.wantIP, c.wantPort, c.wantTLS)
		}
	}
	// trusting everyone leaves nothing to walk back through: the leftmost entry is the client
	ip, _, _ := parseTrustedProxies("*").apply(http.Header{"X-Forwarded-For": {"1.1.1.1, 2.2.2.2"}}, "p", 1, false)
	if ip != "1.1.1.1" {
		t.Errorf("trust all: got %s", ip)
	}
}
