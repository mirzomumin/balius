// Package cli is the balius command line: it mirrors granian's options,
// each of which can also be set through a BALIUS_* environment variable.
package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mirzomumin/balius/internal/config"
	"github.com/mirzomumin/balius/internal/logx"
	"github.com/mirzomumin/balius/internal/supervisor"
)

// option describes one command line option.
type option struct {
	name    string // long name, without dashes
	env     string // environment variable suffix (after BALIUS_)
	meta    string // value placeholder in the help
	help    string
	def     string // default shown in the help
	boolean bool   // --name / --no-name pair
	multi   bool   // may be repeated
	set     func(value string) error
	reset   func() // multi options: drop the values given so far
}

type parser struct {
	cfg     *config.Config
	options []*option
	byName  map[string]*option

	staticRoutes []string
	staticMounts []string
	logConfig    string
}

func (p *parser) add(o *option) {
	p.options = append(p.options, o)
	p.byName[o.name] = o
}

// ---- value parsers ---------------------------------------------------------

func parseBool(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y", "on":
		return true, nil
	case "0", "false", "f", "no", "n", "off", "":
		return false, nil
	}
	return false, fmt.Errorf("%q is not a valid boolean", v)
}

// noLimit is the upper bound of unbounded integer options.
const noLimit = math.MaxInt

// maxSeconds is the longest duration a time.Duration can hold.
const maxSeconds = int(math.MaxInt64 / int64(time.Second))

func inRange(n, lo, hi int) (int, error) {
	switch {
	case n >= lo && n <= hi:
		return n, nil
	case hi == noLimit:
		return 0, fmt.Errorf("%d is not in the range x>=%d", n, lo)
	}
	return 0, fmt.Errorf("%d is not in the range %d<=x<=%d", n, lo, hi)
}

func parseInt(v string, lo, hi int) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("%q is not a valid integer", v)
	}
	return inRange(n, lo, hi)
}

var durationUnits = map[byte]int{'s': 1, 'm': 60, 'h': 3600, 'd': 86400}

// parseDuration reads a number of seconds, or a number followed by one of
// the s, m, h, d suffixes.
func parseDuration(v string, lo, hi int) (int, error) {
	v = strings.TrimSpace(v)
	mult := 1
	if v != "" {
		if m, ok := durationUnits[v[len(v)-1]]; ok {
			mult, v = m, v[:len(v)-1]
		}
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%q is not a valid duration", v)
	}
	hi = min(hi, maxSeconds)
	if n > hi/mult {
		return inRange(hi+1, lo, hi) // would overflow: out of range anyway
	}
	return inRange(n*mult, lo, hi)
}

func parseSeconds(v string) (float64, error) {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil || math.IsNaN(f) || f < 0 || f > float64(maxSeconds) {
		return 0, fmt.Errorf("%q is not a valid number of seconds", v)
	}
	return f, nil
}

// ---- option builders -------------------------------------------------------

func envName(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

// opt registers an option whose values parse into *dst.
func opt[T any](p *parser, o option, dst *T, parse func(string) (T, error)) *option {
	o.env = envName(o.name)
	o.set = func(v string) error {
		x, err := parse(v)
		if err == nil {
			*dst = x
		}
		return err
	}
	p.add(&o)
	return &o
}

func (p *parser) str(name, help, def string, dst *string) *option {
	return opt(p, option{name: name, meta: "TEXT", help: help, def: def}, dst,
		func(v string) (string, error) { return v, nil })
}

func (p *parser) list(name, meta, help string, dst *[]string) *option {
	o := &option{
		name: name, env: envName(name), meta: meta, help: help, multi: true,
		set:   func(v string) error { *dst = append(*dst, v); return nil },
		reset: func() { *dst = nil },
	}
	p.add(o)
	return o
}

func (p *parser) num(name, help, def string, lo, hi int, dst *int) {
	opt(p, option{name: name, meta: "INTEGER", help: help, def: def}, dst,
		func(v string) (int, error) { return parseInt(v, lo, hi) })
}

func (p *parser) duration(name, help, def string, lo, hi int, dst *int) {
	opt(p, option{name: name, meta: "DURATION", help: help, def: def}, dst,
		func(v string) (int, error) { return parseDuration(v, lo, hi) })
}

func (p *parser) flag(name, help, def string, dst *bool) *option {
	return opt(p, option{name: name, help: help, def: def, boolean: true}, dst, parseBool)
}

func (p *parser) choice(name, help, def string, choices []string, dst *string) {
	opt(p, option{name: name, meta: "[" + strings.Join(choices, "|") + "]", help: help, def: def}, dst,
		func(v string) (string, error) {
			if slices.Contains(choices, v) {
				return v, nil
			}
			return "", fmt.Errorf("%q is not one of %s", v, strings.Join(choices, ", "))
		})
}

func newParser() *parser {
	cfg := config.Default()
	p := &parser{cfg: cfg, byName: map[string]*option{}}
	const inf = noLimit
	// granian options without an equivalent here: validated, then ignored
	var ignored struct {
		n int
		b bool
		s string
	}

	p.str("host", "Host address to bind to", "127.0.0.1", &cfg.Host)
	p.num("port", "Port to bind to.", "8000", 0, 65535, &cfg.Port)
	p.str("uds", "Unix Domain Socket to bind to.", "", &cfg.UDS).meta = "PATH"
	opt(p, option{name: "uds-permissions", meta: "OCTAL INTEGER", help: "Unix Domain Socket file permissions"},
		&cfg.UDSPermissions, func(v string) (int, error) {
			n, err := strconv.ParseUint(strings.TrimPrefix(v, "0o"), 8, 32)
			if err != nil {
				return 0, fmt.Errorf("%q is not a valid octal integer", v)
			}
			return int(n), nil
		})
	p.choice("interface", "Application interface type", "rsgi",
		[]string{"asgi", "asginl", "rsgi", "wsgi"}, &cfg.Interface)
	p.choice("http", "HTTP version", "auto", []string{"auto", "1", "2"}, &cfg.HTTP)
	p.flag("ws", "Enable websockets handling", "enabled", &cfg.Websocket).env = "WEBSOCKETS"
	p.num("workers", "Number of worker processes", "1", 1, inf, &cfg.Workers)
	p.num("blocking-threads", "Number of blocking threads (per worker)", "", 1, inf, &cfg.BlockingThreads)
	p.duration("blocking-threads-idle-timeout",
		"The maximum amount of time in seconds (or a human-readable duration) an idle blocking thread will be kept alive",
		"30", 5, 600, &cfg.BlockingThreadsIdleTimeout)
	p.num("runtime-threads", "Number of runtime threads (per worker) the Go scheduler runs network I/O on",
		"1", 1, inf, &cfg.RuntimeThreads)
	p.num("runtime-blocking-threads",
		"Accepted for granian compatibility: the Go runtime sizes its blocking I/O threads on its own",
		"", 1, inf, &ignored.n)
	p.choice("runtime-mode",
		"Accepted for granian compatibility: the Go scheduler is always multi-threaded",
		"auto", []string{"auto", "mt", "st"}, &ignored.s)
	p.choice("loop", "Event loop implementation", "auto", []string{"auto", "asyncio", "rloop", "uvloop"}, &cfg.Loop)
	p.choice("task-impl", "Accepted for granian compatibility: tasks always use asyncio", "asyncio", []string{"asyncio", "rust"}, &ignored.s)
	p.num("backlog", "Maximum number of connections to hold in backlog", "1024", 128, inf, &cfg.Backlog)
	p.num("backpressure", "Maximum number of requests to process concurrently (per worker)",
		"backlog/workers", 1, inf, &cfg.Backpressure)

	p.num("http1-buffer-size", "Sets the maximum buffer size for HTTP/1 connections", "417792", 8192, inf,
		&cfg.HTTP1.MaxBufferSize)
	p.num("http1-header-read-timeout", "Sets a timeout (in milliseconds) to read headers", "30000", 1, 60000,
		&cfg.HTTP1.HeaderReadTimeoutMs)
	p.flag("http1-keep-alive", "Enables or disables HTTP/1 keep-alive", "enabled", &cfg.HTTP1.KeepAlive)
	p.flag("http1-pipeline-flush",
		"Accepted for granian compatibility: responses are always flushed as soon as they complete",
		"disabled", &ignored.b)
	p.flag("http2-adaptive-window",
		"Accepted for granian compatibility: flow control windows are managed by the Go HTTP/2 stack",
		"disabled", &ignored.b)
	p.num("http2-initial-connection-window-size", "Sets the max connection-level flow control for HTTP2",
		"1048576", 1024, inf, &cfg.HTTP2.InitialConnectionWindowSize)
	p.num("http2-initial-stream-window-size",
		"Sets the `SETTINGS_INITIAL_WINDOW_SIZE` option for HTTP2 stream-level flow control",
		"1048576", 1024, inf, &cfg.HTTP2.InitialStreamWindowSize)
	p.num("http2-keep-alive-interval",
		"Sets an interval (in milliseconds) for HTTP2 Ping frames should be sent to keep a connection alive",
		"", 1, 60000, &cfg.HTTP2.KeepAliveIntervalMs)
	p.duration("http2-keep-alive-timeout",
		"Sets a timeout (in seconds or a human-readable duration) for receiving an acknowledgement of the HTTP2 keep-alive ping",
		"20", 1, inf, &cfg.HTTP2.KeepAliveTimeoutS)
	p.num("http2-max-concurrent-streams", "Sets the SETTINGS_MAX_CONCURRENT_STREAMS option for HTTP2 connections",
		"200", 10, inf, &cfg.HTTP2.MaxConcurrentStreams)
	p.num("http2-max-frame-size", "Sets the maximum frame size to use for HTTP2", "16384", 1024, inf,
		&cfg.HTTP2.MaxFrameSize)
	p.num("http2-max-headers-size", "Sets the max size of received header frames", "16777216", 1, inf,
		&cfg.HTTP2.MaxHeadersSize)
	p.num("http2-max-send-buffer-size",
		"Accepted for granian compatibility: write buffers are managed by the Go HTTP/2 stack",
		"409600", 1024, inf, &ignored.n)

	p.flag("log", "Enable logging", "enabled", &cfg.LogEnabled).env = "LOG_ENABLED"
	p.choice("log-level", "Log level", "info",
		[]string{"critical", "error", "warning", "warn", "info", "debug", "notset"}, &cfg.LogLevel)
	p.str("log-config", "Logging configuration file (json)", "", &p.logConfig).meta = "FILE"
	p.flag("access-log", "Enable access log", "disabled", &cfg.AccessLog).env = "LOG_ACCESS_ENABLED"
	p.str("access-log-fmt", "Access log format", "", &cfg.AccessLogFmt).env = "LOG_ACCESS_FMT"

	p.str("ssl-certificate", "SSL certificate file", "", &cfg.SSLCert).meta = "FILE"
	p.str("ssl-keyfile", "SSL key file", "", &cfg.SSLKey).meta = "FILE"
	p.str("ssl-keyfile-password", "SSL key password", "", &cfg.SSLKeyPassword)
	p.choice("ssl-protocol-min", "Set the minimum supported protocol for SSL connections.", "tls1.3",
		[]string{"tls1.2", "tls1.3"}, &cfg.SSLProtocolMin)
	p.str("ssl-ca", "Root SSL cerificate file for client verification", "", &cfg.SSLCA).meta = "FILE"
	p.list("ssl-crl", "FILE", "SSL CRL file(s)", &cfg.SSLCRL)
	p.flag("ssl-client-verify", "Verify clients SSL certificates", "disabled", &cfg.SSLClientVerify)

	p.str("url-path-prefix", "URL path prefix the app is mounted on", "", &cfg.URLPathPrefix)

	p.flag("respawn-failed-workers", "Enable workers respawn on unexpected exit", "disabled",
		&cfg.RespawnFailedWorkers)
	opt(p, option{
		name: "respawn-interval", meta: "FLOAT", def: "3.5",
		help: "The number of seconds to sleep between workers respawn",
	}, &cfg.RespawnInterval, parseSeconds)
	p.duration("rss-sample-interval",
		"The sample rate in seconds (or a human-readable duration) for the resource monitor",
		"30", 1, 300, &cfg.RSSSampleInterval)
	p.num("rss-samples", "The number of consecutive samples to consider a worker over resource limit",
		"1", 1, inf, &cfg.RSSSamples)
	p.duration("workers-lifetime",
		"The maximum amount of time in seconds (or a human-readable duration) a worker will be kept alive before respawn",
		"", 60, inf, &cfg.WorkersLifetime)
	p.num("workers-max-rss", "The maximum amount of memory (in MiB) a worker can consume before respawn",
		"", 1, inf, &cfg.WorkersMaxRSS)
	p.duration("workers-kill-timeout",
		"The amount of time in seconds (or a human-readable duration) to wait for killing workers that refused to gracefully stop",
		"disabled", 1, 1800, &cfg.WorkersKillTimeout)

	p.flag("factory", "Treat target as a factory function, that should be invoked to build the actual target",
		"disabled", &cfg.Factory)
	p.str("working-dir", "Set the working directory", "", &cfg.WorkingDir).meta = "DIRECTORY"
	p.list("env-files", "FILE", "Environment file(s) to load", &cfg.EnvFiles)

	p.list("static-path-route", "TEXT", "Route(s) for static file serving", &p.staticRoutes).def = "/static"
	p.list("static-path-mount", "DIRECTORY", "Path(s) to mount for static file serving", &p.staticMounts)
	p.str("static-path-dir-to-file", "Serve the specified file as the index for directory listings", "",
		&cfg.StaticDirToFile)
	p.duration("static-path-expires",
		"Cache headers expiration (in seconds or a human-readable duration) for static file serving. 0 to disable.",
		"86400", 0, inf, &cfg.StaticExpires)

	p.flag("metrics", "Enable the prometheus metrics exporter.", "disabled", &cfg.MetricsEnabled).env = "METRICS_ENABLED"
	p.duration("metrics-scrape-interval", "Configure the interval for metrics collection.", "15", 1, 60,
		&cfg.MetricsScrapeInterval)
	p.str("metrics-address", "Metrics exporter host address to bind to", "127.0.0.1", &cfg.MetricsAddress)
	p.num("metrics-port", "Metrics exporter port to bind to.", "9090", 1, 65535, &cfg.MetricsPort)

	p.flag("reload", "Enable auto reload on application's files changes", "disabled", &cfg.Reload)
	p.list("reload-paths", "PATH", "Paths to watch for changes", &cfg.ReloadPaths).def = "Working directory"
	p.list("reload-ignore-dirs", "TEXT", "Names of directories to ignore changes for. "+
		"Extends the default list of directories to ignore", &cfg.ReloadIgnoreDirs)
	p.list("reload-ignore-patterns", "TEXT", "File/directory name patterns (regex) to ignore changes for. "+
		"Extends the default list of patterns to ignore", &cfg.ReloadIgnorePatterns)
	p.list("reload-ignore-paths", "PATH", "Absolute paths to ignore changes for", &cfg.ReloadIgnorePaths)
	p.num("reload-tick", "The tick frequency (in milliseconds) the reloader watch for changes", "50", 50, 5000,
		&cfg.ReloadTickMs)
	p.flag("reload-ignore-worker-failure", "Ignore worker failures when auto reload is enabled", "disabled",
		&cfg.ReloadIgnoreWorkerFailure)

	p.str("process-name", "Set a custom name for worker processes", "", &cfg.ProcessName)
	p.str("pid-file", "A path to write the PID file to", "", &cfg.PIDFile).meta = "FILE"
	p.str("python", "Python interpreter whose environment (virtualenv) the embedded runtime should use",
		"$VIRTUAL_ENV", &cfg.Python).meta = "PATH"
	return p
}

// ---- parsing ---------------------------------------------------------------

var (
	errHelp    = errors.New("help requested")
	errVersion = errors.New("version requested")
)

// parse applies the environment, then the command line (which wins).
func (p *parser) parse(args []string, getenv func(string) string) error {
	for _, o := range p.options {
		v := getenv("BALIUS_" + o.env)
		if v == "" {
			continue
		}
		values := []string{v}
		if o.multi {
			values = strings.Fields(v)
		}
		for _, value := range values {
			if err := o.set(value); err != nil {
				return config.Errorf("invalid value for BALIUS_%s: %v", o.env, err)
			}
		}
	}

	var positional []string
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--help" || arg == "-h":
			return errHelp
		case arg == "--version":
			return errVersion
		case arg == "--":
			positional = append(positional, args[i+1:]...)
			i = len(args)
			continue
		case !strings.HasPrefix(arg, "--"):
			positional = append(positional, arg)
			continue
		}

		name, value, hasValue := strings.Cut(arg[2:], "=")
		o := p.byName[name]
		negated := false
		if o == nil && strings.HasPrefix(name, "no-") {
			if o = p.byName[name[3:]]; o != nil && o.boolean {
				negated = true
			} else {
				o = nil
			}
		}
		if o == nil {
			return config.Errorf("no such option: --%s", name)
		}
		if o.boolean {
			if hasValue {
				return config.Errorf("option --%s does not take a value", name)
			}
			value = "true"
			if negated {
				value = "false"
			}
		} else if !hasValue {
			if i+1 >= len(args) {
				return config.Errorf("option --%s requires an argument", name)
			}
			i++
			value = args[i]
		}
		if o.multi && !seen[o.name] {
			// the command line replaces what the environment provided
			seen[o.name] = true
			o.reset()
		}
		if err := o.set(value); err != nil {
			return config.Errorf("invalid value for --%s: %v", o.name, err)
		}
	}

	switch len(positional) {
	case 0:
		return config.Errorf("missing argument 'APP'")
	case 1:
		p.cfg.Target = positional[0]
	default:
		return config.Errorf("got unexpected extra argument (%s)", strings.Join(positional[1:], " "))
	}
	return p.finish()
}

// finish resolves the options spanning several flags.
func (p *parser) finish() error {
	cfg := p.cfg
	if p.logConfig != "" {
		data, err := os.ReadFile(p.logConfig)
		if err != nil {
			return config.Errorf("unable to read the logging configuration: %v", err)
		}
		if !json.Valid(data) {
			return config.Errorf("unable to parse provided logging config")
		}
		cfg.LogConfig = string(data)
	}
	switch {
	case len(p.staticMounts) == 0:
	case len(p.staticMounts) == 1 && len(p.staticRoutes) == 0:
		cfg.StaticMounts = []config.StaticMount{{Route: "/static", Path: p.staticMounts[0]}}
	case len(p.staticMounts) != len(p.staticRoutes):
		return config.Errorf("static path routes and mounts should have the same length")
	default:
		for i, mount := range p.staticMounts {
			cfg.StaticMounts = append(cfg.StaticMounts, config.StaticMount{Route: p.staticRoutes[i], Path: mount})
		}
	}
	return nil
}

func (p *parser) help(w io.Writer) {
	fmt.Fprintf(w, "Usage: balius [OPTIONS] APP\n\n  APP  Application target to serve.  [required]\n\nOptions:\n")
	for _, o := range p.options {
		var head string
		if o.boolean {
			head = fmt.Sprintf("--%s / --no-%s", o.name, o.name)
		} else {
			head = fmt.Sprintf("--%s %s", o.name, o.meta)
		}
		extra := "env var: BALIUS_" + o.env
		if o.def != "" {
			extra += "; default: " + o.def
		}
		fmt.Fprintf(w, "  %s\n", head)
		for _, line := range wrap(o.help+"  ["+extra+"]", 68) {
			fmt.Fprintf(w, "        %s\n", line)
		}
	}
	fmt.Fprintf(w, "  --version\n        Show the version and exit.\n  --help\n        Show this message and exit.\n")
}

func wrap(text string, width int) []string {
	var lines []string
	line := ""
	for _, word := range strings.Fields(text) {
		if line != "" && len(line)+1+len(word) > width {
			lines = append(lines, line)
			line = ""
		}
		if line != "" {
			line += " "
		}
		line += word
	}
	if line != "" {
		lines = append(lines, line)
	}
	return lines
}

// Parse turns a command line into a validated configuration.
func Parse(args []string, getenv func(string) string) (*config.Config, []config.Notice, error) {
	p := newParser()
	if err := p.parse(args, getenv); err != nil {
		return nil, nil, err
	}
	notes, err := p.cfg.Normalize()
	return p.cfg, notes, err
}

// Main runs the balius command and returns its exit code.
func Main(args []string) int {
	cfg, notes, err := Parse(args, os.Getenv)
	switch {
	case errors.Is(err, errHelp):
		newParser().help(os.Stdout)
		return 0
	case errors.Is(err, errVersion):
		fmt.Printf("balius %s\n", config.Version)
		return 0
	case err != nil:
		fmt.Fprintf(os.Stderr, "Usage: balius [OPTIONS] APP\nTry 'balius --help' for help.\n\nError: %s\n",
			strings.TrimPrefix(err.Error(), config.ErrConfig.Error()+": "))
		return 2
	}

	logx.Configure(cfg.LogLevel, cfg.LogEnabled)
	for _, note := range notes {
		if note.Warning {
			logx.Warnf("%s", note.Msg)
		} else {
			logx.Infof("%s", note.Msg)
		}
	}
	sup, err := supervisor.New(cfg)
	if err != nil {
		logx.Errorf("%v", err)
		return 1
	}
	return sup.Run()
}
