// Package static serves files straight from the Go runtime, without ever
// entering the Python interpreter.
package static

import (
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mirzomumin/balius/internal/config"
)

// ServerName is the value of the Server response header.
const ServerName = "balius"

// Files is a set of static mounts.
type Files struct {
	mounts    []config.StaticMount
	dirToFile string
	expires   string
}

// New returns nil when no mount is configured.
func New(cfg *config.Config) *Files {
	if len(cfg.StaticMounts) == 0 {
		return nil
	}
	f := &Files{mounts: cfg.StaticMounts, dirToFile: cfg.StaticDirToFile}
	if cfg.StaticExpires > 0 {
		f.expires = "max-age=" + strconv.Itoa(cfg.StaticExpires)
	}
	return f
}

// Result of matching a request path against the mounts.
type Result int

const (
	// NoMatch: the path does not belong to any mount, the application
	// should handle the request.
	NoMatch Result = iota
	// Found: the path maps onto a file to serve.
	Found
	// NotFound: the path belongs to a mount but there is nothing to serve.
	NotFound
)

// Match resolves the (undecoded) request path. Once a mount's route
// matches, the request is the mount's to answer: it never falls through to
// the application, whatever goes wrong resolving it.
func (f *Files) Match(rawPath string) (string, Result) {
	decoded := PercentDecode(rawPath)
	for _, m := range f.mounts {
		rest, ok := strings.CutPrefix(decoded, m.Route)
		if !ok || (rest != "" && rest[0] != '/') {
			continue
		}
		if full, ok := f.resolve(m.Path, m.Path+rest); ok {
			return full, Found
		}
		return "", NotFound
	}
	return "", NoMatch
}

// resolve maps path to the file to serve, if it lies within root.
// Symlinks and dot segments are resolved before the containment check, for
// the directory index too, so neither can lead outside the mount.
func (f *Files) resolve(root, path string) (string, bool) {
	full, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", false
	}
	if st, err := os.Stat(full); err == nil && st.IsDir() {
		if f.dirToFile == "" {
			return "", false
		}
		if full, err = filepath.EvalSymlinks(filepath.Join(full, f.dirToFile)); err != nil {
			return "", false
		}
	}
	return full, full == root || strings.HasPrefix(full, root+string(filepath.Separator))
}

// Serve writes the file at path. It reports whether the file was served.
func (f *Files) Serve(w http.ResponseWriter, path string) bool {
	file, err := os.Open(path)
	if err != nil {
		NotFoundResponse(w)
		return false
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil || st.IsDir() {
		NotFoundResponse(w)
		return false
	}

	h := w.Header()
	h.Set("Server", ServerName)
	if f.expires != "" {
		h.Set("Cache-Control", f.expires)
	}
	if ct := contentType(path); ct != "" {
		h.Set("Content-Type", ct)
	} else {
		h["Content-Type"] = nil // no sniffing
	}
	h.Set("Content-Length", strconv.FormatInt(st.Size(), 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, file)
	return true
}

// contentType guesses the media type of a file from its extension.
func contentType(path string) string {
	ct := mime.TypeByExtension(filepath.Ext(path))
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct
}

// NotFoundResponse writes the canonical 404 response.
func NotFoundResponse(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Server", ServerName)
	h["Content-Type"] = nil
	h.Set("Content-Length", "9")
	w.WriteHeader(http.StatusNotFound)
	_, _ = io.WriteString(w, "Not found")
}

// PercentDecode decodes %XX escapes; malformed escapes are kept verbatim
// and the result is not required to be valid UTF-8.
func PercentDecode(s string) string {
	i := strings.IndexByte(s, '%')
	if i < 0 {
		return s
	}
	out := make([]byte, 0, len(s))
	out = append(out, s[:i]...)
	for ; i < len(s); i++ {
		c := s[i]
		if c == '%' && i+2 < len(s) {
			hi, ok1 := unhex(s[i+1])
			lo, ok2 := unhex(s[i+2])
			if ok1 && ok2 {
				out = append(out, hi<<4|lo)
				i += 2
				continue
			}
		}
		out = append(out, c)
	}
	return string(out)
}

func unhex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}
