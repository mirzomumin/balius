package worker

import (
	"net/http"
	"net/netip"
	"strconv"
	"strings"
)

// trustedProxies decides whose X-Forwarded-* headers to believe. It follows
// uvicorn's rules: entries are IP addresses, networks or literals (anything
// else, such as a unix socket path), `*` trusts every peer.
type trustedProxies struct {
	all      bool
	addrs    map[netip.Addr]struct{}
	nets     []netip.Prefix
	literals map[string]struct{}
}

func parseTrustedProxies(list string) *trustedProxies {
	t := &trustedProxies{addrs: map[netip.Addr]struct{}{}, literals: map[string]struct{}{}}
	for item := range strings.SplitSeq(list, ",") {
		item = strings.TrimSpace(item)
		switch {
		case item == "":
		case item == "*":
			t.all = true
		case strings.Contains(item, "/"):
			if prefix, err := netip.ParsePrefix(item); err == nil {
				t.nets = append(t.nets, prefix.Masked())
			} else {
				t.literals[item] = struct{}{}
			}
		default:
			if addr, err := netip.ParseAddr(item); err == nil {
				t.addrs[addr] = struct{}{}
			} else {
				t.literals[item] = struct{}{}
			}
		}
	}
	return t
}

func (t *trustedProxies) trusts(host string) bool {
	if t.all {
		return true
	}
	if host == "" {
		return false
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		_, ok := t.literals[host]
		return ok
	}
	if _, ok := t.addrs[addr]; ok {
		return true
	}
	for _, prefix := range t.nets {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// forwardedHostPort parses an X-Forwarded-For entry: a bare address,
// `host:port` or `[ipv6]:port`. Anything else is returned as is.
func forwardedHostPort(v string) (string, uint32) {
	host, port := v, ""
	if strings.HasPrefix(v, "[") {
		end := strings.IndexByte(v, ']')
		if end < 0 {
			return v, 0
		}
		rest := v[end+1:]
		if rest != "" && rest[0] != ':' {
			return v, 0
		}
		host, port = v[1:end], strings.TrimPrefix(rest, ":")
	} else if strings.Count(v, ":") == 1 {
		if host, port, _ = strings.Cut(v, ":"); port == "" {
			return v, 0
		}
	}
	if port == "" {
		return host, 0
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		if strings.HasPrefix(v, "[") {
			return host, 0
		}
		return v, 0
	}
	return host, uint32(n)
}

// clientAddr extracts the client from X-Forwarded-For. Every proxy appends
// to the list, so it is read backwards: the client is the first entry that
// isn't one of the trusted proxies (the leftmost one when they all are).
func (t *trustedProxies) clientAddr(forwardedFor string) (string, uint32) {
	entries := strings.Split(forwardedFor, ",")
	if !t.all {
		for i := len(entries) - 1; i >= 0; i-- {
			host, port := forwardedHostPort(strings.TrimSpace(entries[i]))
			if !t.trusts(host) {
				return host, port
			}
		}
	}
	return forwardedHostPort(strings.TrimSpace(entries[0]))
}

// apply returns the client address and scheme the application should see
// for a request relayed by a trusted proxy.
func (t *trustedProxies) apply(h http.Header, ip string, port uint32, tls bool) (string, uint32, bool) {
	if protos := h["X-Forwarded-Proto"]; len(protos) > 0 {
		switch strings.TrimSpace(protos[len(protos)-1]) {
		case "https", "wss":
			tls = true
		case "http", "ws":
			tls = false
		}
	}
	if forwarded := h["X-Forwarded-For"]; len(forwarded) > 0 {
		if host, p := t.clientAddr(strings.Join(forwarded, ", ")); host != "" {
			ip, port = host, p
		}
	}
	return ip, port, tls
}
