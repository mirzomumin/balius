package cli

import (
	"errors"
	"reflect"
	"testing"

	"github.com/mirzomumin/balius/internal/config"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestParseDefaults(t *testing.T) {
	cfg, _, err := Parse([]string{"main:app"}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Target != "main:app" || cfg.Interface != "rsgi" || cfg.Port != 8000 || cfg.Host != "127.0.0.1" {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if cfg.Backpressure != 1024 || cfg.BlockingThreads != 1 || !cfg.Websocket {
		t.Errorf("unexpected derived values: backpressure=%d blocking=%d ws=%v",
			cfg.Backpressure, cfg.BlockingThreads, cfg.Websocket)
	}
}

func TestParseParityOptions(t *testing.T) {
	cfg, _, err := Parse([]string{"app:app"}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KeepAliveTimeout != 5 || cfg.WSPingInterval != 20 || cfg.WSPingTimeout != 20 || cfg.WSMaxSize != 16<<20 ||
		!cfg.WSCompression || !cfg.ProxyHeaders || cfg.ForwardedAllowIPs != "127.0.0.1,::1" ||
		cfg.WorkersMaxRequests != 0 || cfg.WorkersHealthcheckTimeout != 0 || cfg.RuntimeThreads != 0 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	cfg, _, err = Parse([]string{
		"--keep-alive-timeout", "1m", "--ws-ping-interval", "0.5", "--ws-ping-timeout", "2", "--ws-max-size", "1024",
		"--no-ws-per-message-deflate", "--no-proxy-headers", "--forwarded-allow-ips", "*",
		"--workers-max-requests", "1000", "--workers-max-requests-jitter", "50",
		"--workers-healthcheck-timeout", "30s", "--runtime-threads", "2", "app:app",
	}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KeepAliveTimeout != 60 || cfg.WSPingInterval != 0.5 || cfg.WSPingTimeout != 2 || cfg.WSMaxSize != 1024 ||
		cfg.WSCompression || cfg.ProxyHeaders || cfg.ForwardedAllowIPs != "*" ||
		cfg.WorkersMaxRequests != 1000 || cfg.WorkersMaxRequestsJitter != 50 || cfg.WorkersHealthcheckTimeout != 30 ||
		cfg.RuntimeThreads != 2 {
		t.Errorf("flags not applied: %+v", cfg)
	}
}

func TestParseFlags(t *testing.T) {
	cfg, _, err := Parse([]string{
		"--interface=wsgi", "--workers", "4", "--no-http1-keep-alive", "--workers-lifetime", "2h",
		"--static-path-mount", "../../tests/fixtures/static", "--uds-permissions", "0o660",
		"--ssl-crl", "a.pem", "--ssl-crl", "b.pem", "app:app",
	}, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Interface != "wsgi" || cfg.Workers != 4 || cfg.HTTP1.KeepAlive || cfg.WorkersLifetime != 7200 {
		t.Errorf("flags not applied: %+v", cfg)
	}
	// WSGI: blocking threads derive from the backpressure, websockets are off
	if cfg.Backpressure != 256 || cfg.BlockingThreads != 128 || cfg.Websocket {
		t.Errorf("unexpected derived values: %+v", cfg)
	}
	if len(cfg.StaticMounts) != 1 || cfg.StaticMounts[0].Route != "/static" {
		t.Errorf("static mount: %+v", cfg.StaticMounts)
	}
	if cfg.UDSPermissions != 0o660 || len(cfg.SSLCRL) != 2 {
		t.Errorf("uds permissions %o, crl %v", cfg.UDSPermissions, cfg.SSLCRL)
	}
}

func TestParseEnvironment(t *testing.T) {
	cfg, _, err := Parse([]string{"--port", "9000", "app:app"}, env(map[string]string{
		"BALIUS_PORT":       "7000",
		"BALIUS_INTERFACE":  "asgi",
		"BALIUS_WEBSOCKETS": "false",
		"BALIUS_ENV_FILES":  "a.env b.env",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 9000 {
		t.Errorf("the command line should win over the environment, got port %d", cfg.Port)
	}
	if cfg.Interface != "asgi" || cfg.Websocket {
		t.Errorf("environment not applied: %+v", cfg)
	}
	if !reflect.DeepEqual(cfg.EnvFiles, []string{"a.env", "b.env"}) {
		t.Errorf("env files: %v", cfg.EnvFiles)
	}
}

func TestParseErrors(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"a:app", "b:app"},
		{"--interface", "nope", "app:app"},
		{"--workers", "0", "app:app"},
		{"--port"},
		{"--ws=1", "app:app"},
		{"--bogus", "app:app"},
		{"--blocking-threads-idle-timeout", "1", "app:app"},
		{"--respawn-interval", "NaN", "app:app"},
		{"--respawn-interval", "Inf", "app:app"},
		{"--workers-lifetime", "9223372036854775807d", "app:app"}, // would overflow
		{"--workers-lifetime", "300000000000", "app:app"},         // over time.Duration's range
		{"--interface", "rsgi", "--blocking-threads", "4", "app:app"},
		{"--keep-alive-timeout", "0", "app:app"},
		{"--ws-ping-interval", "-1", "app:app"},
		{"--workers-healthcheck-timeout", "1", "app:app"},
		{"--static-path-route", "/a", "--static-path-route", "/b", "--static-path-mount", ".", "app:app"},
	} {
		if _, _, err := Parse(args, env(nil)); !errors.Is(err, config.ErrConfig) {
			t.Errorf("%v: expected a configuration error, got %v", args, err)
		}
	}
}

func TestParseDuration(t *testing.T) {
	for in, want := range map[string]int{"30": 30, "45s": 45, "2m": 120, "1h": 3600, "1d": 86400} {
		got, err := parseDuration(in, 0, noLimit)
		if err != nil || got != want {
			t.Errorf("%s: got %d (%v), want %d", in, got, err, want)
		}
	}
	if _, err := parseDuration("10", 60, noLimit); err == nil {
		t.Error("expected a range error")
	}
	if _, err := parseDuration("abc", 0, noLimit); err == nil {
		t.Error("expected a parse error")
	}
}
