// Package config holds the configuration shared between the balius
// supervisor process and its worker processes.
//
// The supervisor builds a Config from the command line, validates it and
// hands it (as JSON) to every worker it spawns.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Version is the balius release version.
const Version = "0.1.0"

// Interface names.
const (
	InterfaceASGI   = "asgi"
	InterfaceASGINL = "asginl"
	InterfaceRSGI   = "rsgi"
	InterfaceWSGI   = "wsgi"
)

// HTTP modes.
const (
	HTTPAuto = "auto"
	HTTP1    = "1"
	HTTP2    = "2"
)

// DefaultAccessLogFmt is the access log format used when none is given.
const DefaultAccessLogFmt = `[%(time)s] %(addr)s - "%(method)s %(path)s %(protocol)s" %(status)d %(dt_ms).3f`

// HTTP1Settings mirrors the HTTP/1 tunables.
type HTTP1Settings struct {
	HeaderReadTimeoutMs int  `json:"header_read_timeout"`
	KeepAlive           bool `json:"keep_alive"`
	MaxBufferSize       int  `json:"max_buffer_size"`
}

// HTTP2Settings mirrors the HTTP/2 tunables.
type HTTP2Settings struct {
	InitialConnectionWindowSize int `json:"initial_connection_window_size"`
	InitialStreamWindowSize     int `json:"initial_stream_window_size"`
	KeepAliveIntervalMs         int `json:"keep_alive_interval"` // 0 = disabled
	KeepAliveTimeoutS           int `json:"keep_alive_timeout"`
	MaxConcurrentStreams        int `json:"max_concurrent_streams"`
	MaxFrameSize                int `json:"max_frame_size"`
	MaxHeadersSize              int `json:"max_headers_size"`
}

// StaticMount maps a URL prefix onto a directory.
type StaticMount struct {
	Route string `json:"route"`
	Path  string `json:"path"`
}

// Config is the complete server configuration.
type Config struct {
	Target string `json:"target"`

	Host           string `json:"host"`
	Port           int    `json:"port"`
	UDS            string `json:"uds"`
	UDSPermissions int    `json:"uds_permissions"` // -1 = unset

	Interface string `json:"interface"`
	HTTP      string `json:"http"`
	Websocket bool   `json:"websockets"`

	Workers                    int    `json:"workers"`
	BlockingThreads            int    `json:"blocking_threads"` // 0 = auto
	BlockingThreadsIdleTimeout int    `json:"blocking_threads_idle_timeout"`
	RuntimeThreads             int    `json:"runtime_threads"`
	Loop                       string `json:"loop"`

	Backlog      int `json:"backlog"`
	Backpressure int `json:"backpressure"` // 0 = auto

	HTTP1 HTTP1Settings `json:"http1"`
	HTTP2 HTTP2Settings `json:"http2"`
	// KeepAliveTimeout closes connections idle for that many seconds.
	KeepAliveTimeout int `json:"keep_alive_timeout"`

	WSPingInterval float64 `json:"ws_ping_interval"` // seconds, 0 = no pings
	WSPingTimeout  float64 `json:"ws_ping_timeout"`  // seconds, 0 = wait forever
	WSMaxSize      int     `json:"ws_max_size"`      // bytes
	WSCompression  bool    `json:"ws_per_message_deflate"`

	ProxyHeaders      bool   `json:"proxy_headers"`
	ForwardedAllowIPs string `json:"forwarded_allow_ips"`

	LogEnabled   bool   `json:"log_enabled"`
	LogLevel     string `json:"log_level"`
	LogConfig    string `json:"log_config"` // JSON document (already read)
	AccessLog    bool   `json:"access_log"`
	AccessLogFmt string `json:"access_log_fmt"`

	SSLCert         string   `json:"ssl_cert"`
	SSLKey          string   `json:"ssl_key"`
	SSLKeyPassword  string   `json:"ssl_key_password"`
	SSLProtocolMin  string   `json:"ssl_protocol_min"`
	SSLCA           string   `json:"ssl_ca"`
	SSLCRL          []string `json:"ssl_crl"`
	SSLClientVerify bool     `json:"ssl_client_verify"`

	URLPathPrefix string `json:"url_path_prefix"`

	RespawnFailedWorkers bool    `json:"respawn_failed_workers"`
	RespawnInterval      float64 `json:"respawn_interval"`
	RSSSampleInterval    int     `json:"rss_sample_interval"`
	RSSSamples           int     `json:"rss_samples"`
	WorkersLifetime      int     `json:"workers_lifetime"`     // seconds, 0 = unset
	WorkersMaxRSS        int     `json:"workers_max_rss"`      // MiB, 0 = unset
	WorkersKillTimeout   int     `json:"workers_kill_timeout"` // seconds, 0 = unset
	// WorkersMaxRequests recycles a worker after that many requests (0 =
	// never), plus up to WorkersMaxRequestsJitter picked per worker.
	WorkersMaxRequests       int `json:"workers_max_requests"`
	WorkersMaxRequestsJitter int `json:"workers_max_requests_jitter"`
	// WorkersHealthcheckTimeout replaces a worker whose interpreter gave no
	// sign of life for that many seconds (0 = disabled).
	WorkersHealthcheckTimeout int `json:"workers_healthcheck_timeout"`

	Factory    bool     `json:"factory"`
	WorkingDir string   `json:"working_dir"`
	EnvFiles   []string `json:"env_files"`

	StaticMounts    []StaticMount `json:"static_mounts"`
	StaticDirToFile string        `json:"static_dir_to_file"`
	StaticExpires   int           `json:"static_expires"`

	MetricsEnabled        bool   `json:"metrics_enabled"`
	MetricsScrapeInterval int    `json:"metrics_scrape_interval"`
	MetricsAddress        string `json:"metrics_address"`
	MetricsPort           int    `json:"metrics_port"`

	Reload                    bool     `json:"reload"`
	ReloadPaths               []string `json:"reload_paths"`
	ReloadIgnoreDirs          []string `json:"reload_ignore_dirs"`
	ReloadIgnorePatterns      []string `json:"reload_ignore_patterns"`
	ReloadIgnorePaths         []string `json:"reload_ignore_paths"`
	ReloadTickMs              int      `json:"reload_tick"`
	ReloadIgnoreWorkerFailure bool     `json:"reload_ignore_worker_failure"`

	ProcessName string `json:"process_name"`
	PIDFile     string `json:"pid_file"`

	// Python is the interpreter whose environment (virtualenv) the embedded
	// runtime should adopt. Empty means: $VIRTUAL_ENV if set, otherwise the
	// environment libpython was built for.
	Python string `json:"python"`
}

// Default returns a Config holding the same defaults as granian.
func Default() *Config {
	return &Config{
		Host:                       "127.0.0.1",
		Port:                       8000,
		UDSPermissions:             -1,
		Interface:                  InterfaceRSGI,
		HTTP:                       HTTPAuto,
		Websocket:                  true,
		Workers:                    1,
		BlockingThreadsIdleTimeout: 30,
		RuntimeThreads:             1,
		Loop:                       "auto",
		Backlog:                    1024,
		HTTP1: HTTP1Settings{
			HeaderReadTimeoutMs: 30_000,
			KeepAlive:           true,
			MaxBufferSize:       8192 + 4096*100,
		},
		HTTP2: HTTP2Settings{
			InitialConnectionWindowSize: 1024 * 1024,
			InitialStreamWindowSize:     1024 * 1024,
			KeepAliveTimeoutS:           20,
			MaxConcurrentStreams:        200,
			MaxFrameSize:                1024 * 16,
			MaxHeadersSize:              16 * 1024 * 1024,
		},
		KeepAliveTimeout:      5,
		WSPingInterval:        20,
		WSPingTimeout:         20,
		WSMaxSize:             16 * 1024 * 1024,
		WSCompression:         true,
		ProxyHeaders:          true,
		ForwardedAllowIPs:     "127.0.0.1,::1",
		LogEnabled:            true,
		LogLevel:              "info",
		SSLProtocolMin:        "tls1.3",
		RespawnInterval:       3.5,
		RSSSampleInterval:     30,
		RSSSamples:            1,
		StaticExpires:         86400,
		MetricsScrapeInterval: 15,
		MetricsAddress:        "127.0.0.1",
		MetricsPort:           9090,
		ReloadTickMs:          50,
	}
}

// Notice is a non-fatal message produced while normalizing a Config.
type Notice struct {
	Warning bool
	Msg     string
}

// ErrConfig marks configuration errors.
var ErrConfig = errors.New("configuration error")

// Errorf builds an ErrConfig error.
func Errorf(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrConfig, fmt.Sprintf(format, a...))
}

// TLSEnabled reports whether the server terminates TLS.
func (c *Config) TLSEnabled() bool { return c.SSLCert != "" && c.SSLKey != "" }

// Async reports whether the interface runs on an asyncio event loop.
func (c *Config) Async() bool { return c.Interface != InterfaceWSGI }

// BindAddr formats the listening address for humans.
func (c *Config) BindAddr() string {
	if c.UDS != "" {
		return "unix:" + c.UDS
	}
	host := c.Host
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return fmt.Sprintf("%s:%d", host, c.Port)
}

// Normalize validates the configuration and resolves every "auto" value,
// following the rules granian applies in its server constructor and `serve`.
func (c *Config) Normalize() ([]Notice, error) {
	var notes []Notice
	info := func(format string, a ...any) { notes = append(notes, Notice{false, fmt.Sprintf(format, a...)}) }
	warn := func(format string, a ...any) { notes = append(notes, Notice{true, fmt.Sprintf(format, a...)}) }

	if c.Target == "" {
		return notes, Errorf("missing application target")
	}
	if c.Backpressure <= 0 {
		c.Backpressure = max(1, c.Backlog/c.Workers)
	}
	if c.BlockingThreads <= 0 {
		c.BlockingThreads = 1
		if c.Interface == InterfaceWSGI {
			c.BlockingThreads = max(1, c.Backpressure/2)
		}
	}
	if c.AccessLogFmt == "" {
		c.AccessLogFmt = DefaultAccessLogFmt
	}

	if c.UDS != "" {
		abs, err := filepath.Abs(c.UDS)
		if err != nil {
			return notes, Errorf("invalid uds path: %v", err)
		}
		c.UDS = abs
	}

	if c.Interface != InterfaceWSGI && c.BlockingThreads > 1 {
		return notes, Errorf("blocking threads > 1 is not supported on ASGI and RSGI")
	}
	if c.Interface == InterfaceWSGI && c.BlockingThreads > runtime.NumCPU()*2+1 {
		warn("Configuration allows spawning up to %d Python threads, which seems quite high compared to "+
			"the number of CPU cores available. Consider reviewing your configuration and using `backpressure` "+
			"to limit the concurrency on the Python interpreter. If this configuration is intentional, "+
			"you can safely ignore this message.", c.BlockingThreads)
	}
	if c.Websocket {
		if c.Interface == InterfaceWSGI {
			c.Websocket = false
			info("Websockets are not supported on WSGI, ignoring")
		} else if c.HTTP == HTTP2 {
			c.Websocket = false
			info("Websockets are not supported on HTTP/2 only, ignoring")
		}
	}
	if c.WorkersLifetime != 0 {
		if c.Reload {
			c.WorkersLifetime = 0
			info("Workers lifetime is not available in combination with changes reloader, ignoring")
		}
	}
	if c.WorkersMaxRSS != 0 && c.Reload {
		c.WorkersMaxRSS = 0
		info("The resource monitor is not available in combination with changes reloader, ignoring")
	}
	if c.MetricsEnabled && c.Reload {
		c.MetricsEnabled = false
		info("Metrics are not available in combination with changes reloader, ignoring")
	}

	// TLS
	if (c.SSLCert == "") != (c.SSLKey == "") {
		return notes, Errorf("both --ssl-certificate and --ssl-keyfile are required to enable TLS")
	}
	for _, p := range []*string{&c.SSLCert, &c.SSLKey, &c.SSLCA} {
		if *p == "" {
			continue
		}
		abs, err := filepath.Abs(*p)
		if err != nil {
			return notes, Errorf("invalid path %q: %v", *p, err)
		}
		*p = abs
	}
	for i, p := range c.SSLCRL {
		if abs, err := filepath.Abs(p); err == nil {
			c.SSLCRL[i] = abs
		}
	}
	if c.SSLClientVerify && c.SSLCA == "" {
		warn("SSL client verification requires a CA certificate, ignoring")
		c.SSLClientVerify = false
	}

	// Static files
	for i := range c.StaticMounts {
		m := &c.StaticMounts[i]
		abs, err := filepath.Abs(m.Path)
		if err != nil {
			return notes, Errorf("invalid static path %q: %v", m.Path, err)
		}
		if resolved, err := filepath.EvalSymlinks(abs); err == nil {
			abs = resolved
		}
		st, err := os.Stat(abs)
		if err != nil || !st.IsDir() {
			return notes, Errorf("static path mount %q is not a directory", m.Path)
		}
		m.Path = abs
	}

	cpus := runtime.NumCPU()
	if c.Workers > cpus {
		warn("Configured number of workers appears to be higher than the amount of CPU cores available. "+
			"Mind that such value might actually decrease the overall throughput of the server. "+
			"Consider using %d workers and tune threads configuration instead", cpus)
	}
	if c.RuntimeThreads > cpus {
		warn("Configured number of runtime threads appears to be too high given the amount of CPU cores " +
			"available. Mind that runtime threads are not involved in Python code execution, and they almost " +
			"never be the limiting factor in scaling. Consider configuring the amount of blocking threads instead")
	}
	return notes, nil
}
