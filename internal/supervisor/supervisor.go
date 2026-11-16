// Package supervisor is the tulpar main process: it owns the listening
// socket, spawns the worker processes and keeps them healthy.
//
// It never touches Python: workers are copies of this same executable,
// started with a marker in their environment and their configuration on an
// inherited pipe.
package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mirzomumin/tulpar/internal/config"
	"github.com/mirzomumin/tulpar/internal/logx"
	"github.com/mirzomumin/tulpar/internal/metrics"
	"github.com/mirzomumin/tulpar/internal/netx"
	"github.com/mirzomumin/tulpar/internal/worker"
)

// EnvWorker marks a process as a worker.
const EnvWorker = "TULPAR_WORKER"

// crashLoopWindow is how soon after a respawn a new failure of the same
// worker is considered a crash loop.
const crashLoopWindow = 5500 * time.Millisecond

// proc is a running worker process.
type proc struct {
	idx     int
	cmd     *exec.Cmd
	control *os.File // write end of the worker's control pipe
	birth   time.Time
	exited  chan struct{}
	// set when the supervisor itself asked the worker to stop
	interrupted atomic.Bool
}

func (p *proc) pid() int { return p.cmd.Process.Pid }

func (p *proc) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func (p *proc) terminate() {
	p.interrupted.Store(true)
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
}

func (p *proc) kill() {
	p.interrupted.Store(true)
	_ = p.cmd.Process.Kill()
}

// join waits for the process to exit; a zero timeout waits forever. It
// reports whether the process is gone.
func (p *proc) join(timeout time.Duration) bool {
	if timeout <= 0 {
		<-p.exited
		return true
	}
	select {
	case <-p.exited:
		return true
	case <-time.After(timeout):
		return false
	}
}

// Supervisor manages the workers of one server.
type Supervisor struct {
	cfg *config.Config
	exe string

	sock   *os.File // port reservation (per-worker sockets) or the shared socket
	shared bool

	baseEnv []string
	env     []string

	workers  []*proc
	exits    chan *proc
	signals  chan os.Signal
	lifetime <-chan time.Time
	rssTick  <-chan time.Time

	respawned    map[int]time.Time
	rssSamples   map[int]int
	failed       atomic.Bool // a worker died and wasn't replaced
	interrupted  bool
	reloadSignal bool

	metrics  *metrics.Aggregator
	exporter *exporter
	pid      int
}

// New prepares a supervisor for a normalized configuration.
func New(cfg *config.Config) (*Supervisor, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("unable to locate the tulpar executable: %w", err)
	}
	return &Supervisor{
		cfg:        cfg,
		exe:        exe,
		baseEnv:    workerEnviron(os.Environ()),
		exits:      make(chan *proc, 64),
		signals:    make(chan os.Signal, 8),
		respawned:  map[int]time.Time{},
		rssSamples: map[int]int{},
		metrics:    metrics.NewAggregator(cfg.Workers),
		pid:        os.Getpid(),
	}, nil
}

// Run serves until interrupted. It returns the process exit code.
func (s *Supervisor) Run() int {
	if err := s.startup(); err != nil {
		logx.Errorf("%v", err)
		s.shutdown()
		return 1
	}
	if s.cfg.Reload {
		s.serveWithReloader()
	} else {
		s.serve()
	}
	s.shutdown()
	if s.failed.Load() {
		return 1
	}
	return 0
}

func (s *Supervisor) startup() error {
	logx.Infof("Starting tulpar (main PID: %d)", s.pid)
	if err := s.writePIDFile(); err != nil {
		return err
	}
	signal.Notify(s.signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	if err := s.initSocket(); err != nil {
		return err
	}
	proto := "http"
	if s.cfg.TLSEnabled() {
		proto = "https"
	}
	logx.Infof("Listening at: %s://%s", proto, s.cfg.BindAddr())

	if err := s.loadEnv(); err != nil {
		return err
	}
	if err := s.spawnWorkers(); err != nil {
		return err
	}
	if s.cfg.WorkersLifetime > 0 {
		s.lifetime = time.After(time.Duration(s.cfg.WorkersLifetime) * time.Second)
	}
	if s.cfg.WorkersMaxRSS > 0 {
		s.rssTick = time.Tick(time.Duration(s.cfg.RSSSampleInterval) * time.Second)
	}
	if s.cfg.MetricsEnabled {
		exp, err := startExporter(s.cfg.MetricsAddress, s.cfg.MetricsPort, s.metrics)
		if err != nil {
			return fmt.Errorf("unable to start the metrics exporter: %w", err)
		}
		s.exporter = exp
	}
	return nil
}

// initSocket binds the listening address.
//
// With TCP on platforms balancing SO_REUSEPORT sockets every worker opens
// its own listening socket; the supervisor only keeps a bound (never
// listening) one, which reserves the port and resolves port 0. Everywhere
// else, and for unix sockets, workers share the supervisor's socket.
func (s *Supervisor) initSocket() error {
	cfg := s.cfg
	if cfg.UDS != "" {
		f, err := netx.UnixSocket(cfg.UDS, cfg.UDSPermissions)
		if err != nil {
			return err
		}
		s.sock, s.shared = f, true
		return nil
	}
	f, port, err := netx.TCPSocket(cfg.Host, cfg.Port)
	if err != nil {
		return err
	}
	cfg.Port = port
	s.sock, s.shared = f, !netx.ReusePortBalanced
	return nil
}

func (s *Supervisor) shutdown() {
	logx.Infof("Shutting down tulpar")
	signal.Stop(s.signals)
	if s.exporter != nil {
		s.exporter.stop()
	}
	if s.sock != nil {
		s.sock.Close()
	}
	s.stopWorkers()
	s.unlinkFiles()
	logx.Infof("tulpar shutdown completed, see ya!")
}

// workerEnviron is the environment handed to workers: the configuration
// reaches them over their control pipe, so the TULPAR_* settings (the TLS key
// password among them) are kept away from the application.
func workerEnviron(env []string) []string {
	return slices.DeleteFunc(env, func(kv string) bool {
		return strings.HasPrefix(kv, "TULPAR_") && !strings.HasPrefix(kv, "TULPAR_CPUPROFILE=")
	})
}

// ---- workers ---------------------------------------------------------------

func (s *Supervisor) spawn(idx int) (*proc, error) {
	spec := worker.Spec{ID: idx + 1, Config: s.cfg, SharedSocket: s.shared}
	payload, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("encode worker spec: %w", err)
	}
	controlR, controlW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	ipcR, ipcW, err := os.Pipe()
	if err != nil {
		controlR.Close()
		controlW.Close()
		return nil, err
	}

	name := s.cfg.ProcessName
	if name == "" {
		name = "tulpar"
	}
	cmd := &exec.Cmd{
		Path:   s.exe,
		Args:   []string{fmt.Sprintf("%s worker-%d", name, idx+1)},
		Env:    append(append([]string{}, s.env...), EnvWorker+"=1"),
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		// fds 3, 4 and 5 in the worker (see worker.Fd*)
		ExtraFiles: []*os.File{controlR, ipcW},
	}
	if s.shared {
		cmd.ExtraFiles = append(cmd.ExtraFiles, s.sock)
	}
	err = cmd.Start()
	controlR.Close()
	ipcW.Close()
	if err != nil {
		controlW.Close()
		ipcR.Close()
		return nil, fmt.Errorf("unable to spawn worker-%d: %w", idx+1, err)
	}

	p := &proc{idx: idx, cmd: cmd, control: controlW, birth: time.Now(), exited: make(chan struct{})}
	// The pipe stays open for the life of the supervisor: the worker reads
	// EOF on it when we are gone, however that happened.
	if _, err := controlW.Write(append(payload, '\n')); err != nil {
		logx.Errorf("Unable to configure worker-%d: %v", idx+1, err)
	}
	go func() {
		_ = cmd.Wait()
		controlW.Close()
		close(p.exited)
		s.exits <- p
	}()
	go s.collectMetrics(idx, ipcR)

	logx.Infof("Spawning worker-%d with PID: %d", idx+1, p.pid())
	return p, nil
}

// collectMetrics reads the snapshots a worker reports on its IPC pipe.
func (s *Supervisor) collectMetrics(idx int, ipc *os.File) {
	defer ipc.Close()
	dec := json.NewDecoder(ipc)
	for {
		var snap metrics.Snapshot
		if err := dec.Decode(&snap); err != nil {
			return
		}
		s.metrics.Collect(idx, snap)
	}
}

func (s *Supervisor) spawnWorkers() error {
	for idx := 0; idx < s.cfg.Workers; idx++ {
		p, err := s.spawn(idx)
		if err != nil {
			return err
		}
		s.workers = append(s.workers, p)
	}
	s.metrics.Spawns.Add(int64(s.cfg.Workers))
	return nil
}

func (s *Supervisor) killTimeout() time.Duration {
	return time.Duration(s.cfg.WorkersKillTimeout) * time.Second
}

// reap waits for a terminated worker, killing it when it outlives timeout.
func (s *Supervisor) reap(p *proc, timeout time.Duration, label string) {
	if p.join(timeout) {
		return
	}
	logx.Warnf("Killing %sworker-%d after it refused to gracefully stop", label, p.idx+1)
	p.kill()
	p.join(0)
}

// respawnWorkers replaces workers one at a time: the new one is started,
// given `delay` to come up, then the old one is gracefully stopped.
func (s *Supervisor) respawnWorkers(idxs []int, delay time.Duration) {
	for _, idx := range idxs {
		if idx >= len(s.workers) {
			continue
		}
		s.respawned[idx] = time.Now()
		logx.Infof("Respawning worker-%d", idx+1)
		old := s.workers[idx]
		p, err := s.spawn(idx)
		if err != nil {
			logx.Errorf("%v", err)
			continue
		}
		s.workers[idx] = p
		s.metrics.Spawns.Add(1)
		if delay > 0 && s.sleep(delay) {
			// interrupted: the whole server is going down anyway
			old.terminate()
			s.reap(old, s.killTimeout(), "old ")
			return
		}
		if !old.alive() {
			continue
		}
		logx.Infof("Stopping old worker-%d", idx+1)
		old.terminate()
		s.reap(old, s.killTimeout(), "old ")
		logx.Infof("Stopped old worker-%d", idx+1)
	}
}

// sleep waits for d; it returns true when an interrupt arrived meanwhile.
func (s *Supervisor) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			return false
		case sig := <-s.signals:
			if sig == syscall.SIGHUP {
				s.reloadSignal = true
				continue
			}
			s.interrupted = true
			return true
		}
	}
}

func (s *Supervisor) stopWorkers() {
	for _, p := range s.workers {
		p.terminate()
	}
	deadline := time.Now().Add(s.killTimeout())
	for _, p := range s.workers {
		timeout := time.Duration(0)
		if s.cfg.WorkersKillTimeout > 0 {
			timeout = max(time.Until(deadline), time.Millisecond)
		}
		// a second interrupt while waiting means "stop now"
		done := make(chan struct{})
		go func() {
			s.reap(p, timeout, "")
			close(done)
		}()
	wait:
		for {
			select {
			case <-done:
				break wait
			case sig := <-s.signals:
				if sig != syscall.SIGHUP {
					logx.Warnf("Forcing workers to stop")
					for _, w := range s.workers {
						w.kill()
					}
				}
			}
		}
		logx.Infof("Stopped worker-%d", p.idx+1)
	}
	s.workers = nil
}

// ---- main loops ------------------------------------------------------------

// pendingInterrupt checks, without blocking, whether a stop signal is
// waiting to be handled.
func (s *Supervisor) pendingInterrupt() bool {
	for {
		select {
		case sig := <-s.signals:
			if sig == syscall.SIGHUP {
				s.reloadSignal = true
				continue
			}
			s.interrupted = true
			return true
		default:
			return s.interrupted
		}
	}
}

// current reports whether p is still the process in charge of its slot.
func (s *Supervisor) current(p *proc) bool {
	return p.idx < len(s.workers) && s.workers[p.idx] == p
}

func (s *Supervisor) serve() {
	for !s.interrupted {
		select {
		case sig := <-s.signals:
			if sig == syscall.SIGHUP {
				s.reload()
				continue
			}
			s.interrupted = true

		case p := <-s.exits:
			if p.interrupted.Load() || !s.current(p) {
				continue
			}
			// On ^C the whole process group gets the signal: a worker may
			// be gone before we looked at our own copy of it.
			if s.pendingInterrupt() {
				continue
			}
			logx.Errorf("Unexpected exit from worker-%d", p.idx+1)
			if !s.cfg.RespawnFailedWorkers {
				s.failed.Store(true)
				return
			}
			if last, ok := s.respawned[p.idx]; ok && time.Since(last) <= crashLoopWindow {
				logx.Errorf("Worker crash loop detected, exiting")
				s.failed.Store(true)
				return
			}
			s.respawned = map[int]time.Time{}
			s.respawnWorkers([]int{p.idx}, 0)
			s.metrics.RespawnsErr.Add(1)

		case <-s.lifetime:
			s.handleLifetime()

		case <-s.rssTick:
			s.handleRSS()
		}
		if s.reloadSignal && !s.interrupted {
			s.reload()
		}
	}
}

func (s *Supervisor) reload() {
	logx.Infof("HUP signal received, gracefully respawning workers..")
	s.reloadSignal = false
	s.respawned = map[int]time.Time{}
	if err := s.loadEnv(); err != nil {
		logx.Errorf("%v", err)
	}
	idxs := make([]int, len(s.workers))
	for i := range idxs {
		idxs[i] = i
	}
	s.respawnWorkers(idxs, s.respawnDelay())
}

func (s *Supervisor) respawnDelay() time.Duration {
	return time.Duration(s.cfg.RespawnInterval * float64(time.Second))
}

func (s *Supervisor) handleLifetime() {
	lifetime := time.Duration(s.cfg.WorkersLifetime) * time.Second
	ttl := lifetime * 95 / 100
	next := lifetime
	for _, p := range append([]*proc{}, s.workers...) {
		if s.interrupted {
			return
		}
		age := time.Since(p.birth)
		if age >= ttl {
			logx.Infof("worker-%d lifetime expired, gracefully respawning..", p.idx+1)
			s.respawnWorkers([]int{p.idx}, s.respawnDelay())
			s.metrics.RespawnsTTL.Add(1)
			continue
		}
		next = min(next, max(time.Minute, lifetime-age))
	}
	s.lifetime = time.After(next)
}

func (s *Supervisor) handleRSS() {
	limit := int64(s.cfg.WorkersMaxRSS) * 1024 * 1024
	samples := map[int]int{}
	var restart []int
	for _, p := range s.workers {
		rss, err := processRSS(p.pid())
		if err != nil {
			logx.Warnf("Unable to collect resource usage for workers")
			return
		}
		logx.Debugf("Collected resource usage for worker-%d: %d bytes", p.idx+1, rss)
		if rss < limit {
			continue
		}
		n := s.rssSamples[p.pid()] + 1
		if n >= s.cfg.RSSSamples {
			logx.Infof("worker-%d RSS over threshold, gracefully respawning..", p.idx+1)
			restart = append(restart, p.idx)
		} else {
			samples[p.pid()] = n
		}
	}
	s.rssSamples = samples
	if len(restart) > 0 {
		s.respawnWorkers(restart, s.respawnDelay())
		s.metrics.RespawnsRSS.Add(int64(len(restart)))
	}
}

// ---- files -----------------------------------------------------------------

func (s *Supervisor) writePIDFile() error {
	path := s.cfg.PIDFile
	if path == "" {
		return nil
	}
	if data, err := os.ReadFile(path); err == nil {
		var existing int
		if _, err := fmt.Sscanf(string(data), "%d", &existing); err != nil {
			return fmt.Errorf("unable to read existing PID file %s", path)
		}
		if existing != s.pid {
			if err := syscall.Kill(existing, 0); err == nil || !errors.Is(err, syscall.ESRCH) {
				return fmt.Errorf("the PID file %s already exists for %d", path, existing)
			}
		}
	}
	if err := os.WriteFile(path, []byte(fmt.Sprint(s.pid)), 0o644); err != nil {
		return fmt.Errorf("unable to write PID file: %w", err)
	}
	return nil
}

func (s *Supervisor) unlinkFiles() {
	// only a socket this instance bound is its to remove: a start that failed
	// (e.g. because another instance serves on it) must leave it alone
	if s.cfg.UDS != "" && s.sock != nil {
		_ = os.Remove(s.cfg.UDS)
	}
	path := s.cfg.PIDFile
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var pid int
	if _, err := fmt.Sscanf(string(data), "%d", &pid); err != nil {
		logx.Errorf("Unable to read PID file %s", path)
		return
	}
	if pid == s.pid {
		_ = os.Remove(path)
	}
}
