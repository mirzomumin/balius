package supervisor

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/mirzomumin/tulpar/internal/logx"
)

// Changes spread over more than this are reported in several batches.
const reloadDebounce = 1600 * time.Millisecond

// Defaults mirroring watchfiles' DefaultFilter.
var (
	defaultIgnoreDirs = []string{
		"__pycache__", ".git", ".hg", ".svn", ".tox", ".venv", ".idea", "node_modules",
		".mypy_cache", ".pytest_cache", ".hypothesis", "site-packages",
	}
	defaultIgnorePatterns = []string{
		`\.py[cod]$`, `\.___jb_...___$`, `\.sw.$`, `~$`, `^\.\#`, `^\.DS_Store$`, `^flycheck_`,
	}
)

// changeFilter decides which filesystem changes trigger a reload.
type changeFilter struct {
	dirs     map[string]bool
	patterns []*regexp.Regexp
	paths    []string
}

func newChangeFilter(dirs, patterns, paths []string) (*changeFilter, error) {
	f := &changeFilter{dirs: map[string]bool{}}
	for _, d := range append(append([]string{}, defaultIgnoreDirs...), dirs...) {
		f.dirs[d] = true
	}
	for _, p := range append(append([]string{}, defaultIgnorePatterns...), patterns...) {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, err
		}
		f.patterns = append(f.patterns, re)
	}
	for _, p := range paths {
		if abs, err := filepath.Abs(p); err == nil {
			f.paths = append(f.paths, abs)
		}
	}
	return f, nil
}

func (f *changeFilter) ignoredDir(path string) bool {
	if f.dirs[filepath.Base(path)] {
		return true
	}
	return f.ignoredPath(path)
}

func (f *changeFilter) ignoredPath(path string) bool {
	for _, p := range f.paths {
		if path == p || strings.HasPrefix(path, p+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// accepts reports whether a change to path matters.
func (f *changeFilter) accepts(path string) bool {
	for _, part := range strings.Split(filepath.Dir(path), string(filepath.Separator)) {
		if f.dirs[part] {
			return false
		}
	}
	name := filepath.Base(path)
	for _, re := range f.patterns {
		if re.MatchString(name) {
			return false
		}
	}
	return !f.ignoredPath(path)
}

type changeWatcher struct {
	fs     *fsnotify.Watcher
	filter *changeFilter
}

func newChangeWatcher(roots []string, filter *changeFilter) (*changeWatcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &changeWatcher{fs: fsw, filter: filter}
	for _, root := range roots {
		abs, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		w.addTree(abs)
	}
	return w, nil
}

// addTree watches a directory and everything below it.
func (w *changeWatcher) addTree(root string) {
	st, err := os.Stat(root)
	if err != nil {
		return
	}
	if !st.IsDir() {
		_ = w.fs.Add(root)
		return
	}
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			return nil
		}
		if path != root && w.filter.ignoredDir(path) {
			return filepath.SkipDir
		}
		_ = w.fs.Add(path)
		return nil
	})
}

func changeKind(op fsnotify.Op) string {
	switch {
	case op.Has(fsnotify.Create):
		return "Added"
	case op.Has(fsnotify.Remove), op.Has(fsnotify.Rename):
		return "Deleted"
	default:
		return "Modified"
	}
}

// next blocks until a batch of changes is ready, or the supervisor has to
// act on a signal or a failed worker (nil batch).
// A batch closes once no change was seen for `step`, or after the
// debounce window.
func (w *changeWatcher) next(step time.Duration, s *Supervisor) (map[string]string, os.Signal) {
	changes := map[string]string{}
	var quiet <-chan time.Time
	var deadline <-chan time.Time
	for {
		fire := false
		select {
		case sig := <-s.signals:
			return nil, sig
		case p := <-s.exits:
			// worker failures are reported, and fatal unless told otherwise
			if p.interrupted.Load() {
				continue
			}
			logx.Errorf("Unexpected exit from worker-%d", p.idx+1)
			if !s.cfg.ReloadIgnoreWorkerFailure {
				s.failed.Store(true)
				return nil, syscall.SIGTERM
			}
		case ev, ok := <-w.fs.Events:
			if !ok {
				return nil, nil
			}
			if ev.Op.Has(fsnotify.Create) {
				if st, err := os.Stat(ev.Name); err == nil && st.IsDir() {
					if !w.filter.ignoredDir(ev.Name) {
						w.addTree(ev.Name)
					}
					continue
				}
			}
			if ev.Op == fsnotify.Chmod || !w.filter.accepts(ev.Name) {
				continue
			}
			if _, seen := changes[ev.Name]; !seen || ev.Op.Has(fsnotify.Remove) {
				changes[ev.Name] = changeKind(ev.Op)
			}
			quiet = time.After(step)
			if deadline == nil {
				deadline = time.After(reloadDebounce)
			}
		case err := <-w.fs.Errors:
			logx.Debugf("file watcher: %v", err)
		case <-quiet:
			fire = true
		case <-deadline:
			fire = true
		}
		if !fire {
			continue
		}
		// files created and gone within the batch (editors' and tools'
		// temporaries) are noise
		for path, kind := range changes {
			if kind == "Added" {
				if _, err := os.Lstat(path); err != nil {
					delete(changes, path)
				}
			}
		}
		if len(changes) == 0 {
			quiet, deadline = nil, nil
			continue
		}
		return changes, nil
	}
}

// serveWithReloader restarts every worker whenever the watched files change.
func (s *Supervisor) serveWithReloader() {
	filter, err := newChangeFilter(s.cfg.ReloadIgnoreDirs, s.cfg.ReloadIgnorePatterns, s.cfg.ReloadIgnorePaths)
	if err != nil {
		logx.Errorf("Invalid reload ignore pattern: %v", err)
		s.failed.Store(true)
		return
	}
	roots := s.cfg.ReloadPaths
	if len(roots) == 0 {
		roots = []string{"."}
	}
	watcher, err := newChangeWatcher(roots, filter)
	if err != nil {
		logx.Errorf("Unable to watch for changes: %v", err)
		s.failed.Store(true)
		return
	}
	defer watcher.fs.Close()
	step := time.Duration(s.cfg.ReloadTickMs) * time.Millisecond

	for {
		changes, sig := watcher.next(step, s)
		if changes == nil {
			if sig == syscall.SIGHUP {
				s.reload()
				continue
			}
			return
		}
		logx.Infof("Changes detected, reloading workers..")
		paths := make([]string, 0, len(changes))
		for path := range changes {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			logx.Infof("%s: %s", changes[path], path)
		}
		if err := s.loadEnv(); err != nil {
			logx.Errorf("%v", err)
		}
		s.stopWorkers()
		if err := s.spawnWorkers(); err != nil {
			logx.Errorf("%v", err)
			s.failed.Store(true)
			return
		}
	}
}
