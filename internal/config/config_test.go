package config

import (
	"errors"
	"testing"
)

func TestNormalizeIgnoresUnsupportedCombinations(t *testing.T) {
	c := Default()
	c.Target, c.HTTP = "app:app", HTTP2
	if _, err := c.Normalize(); err != nil {
		t.Fatal(err)
	}
	if c.Websocket {
		t.Error("websockets can't run on HTTP/2 only")
	}

	c = Default()
	c.Target, c.Reload, c.WorkersLifetime, c.MetricsEnabled = "app:app", true, 120, true
	notes, err := c.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkersLifetime != 0 || c.MetricsEnabled || len(notes) < 2 {
		t.Errorf("reload must disable lifetime and metrics: %+v %v", c, notes)
	}
}

func TestNormalizeErrors(t *testing.T) {
	for name, mutate := range map[string]func(*Config){
		"no target":  func(c *Config) { c.Target = "" },
		"cert only":  func(c *Config) { c.SSLCert = "cert.pem" },
		"bad static": func(c *Config) { c.StaticMounts = []StaticMount{{"/s", "/does/not/exist"}} },
	} {
		c := Default()
		c.Target = "app:app"
		mutate(c)
		if _, err := c.Normalize(); !errors.Is(err, ErrConfig) {
			t.Errorf("%s: expected a configuration error, got %v", name, err)
		}
	}
}

func TestBindAddr(t *testing.T) {
	c := Default()
	c.Host = "::1"
	if got := c.BindAddr(); got != "[::1]:8000" {
		t.Errorf("got %s", got)
	}
	c.UDS = "/run/app.sock"
	if got := c.BindAddr(); got != "unix:/run/app.sock" {
		t.Errorf("got %s", got)
	}
}
