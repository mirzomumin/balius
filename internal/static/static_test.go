package static

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mirzomumin/balius/internal/config"
)

func newFiles(t *testing.T, dirToFile string) (*Files, string) {
	t.Helper()
	root := t.TempDir()
	mount := filepath.Join(root, "public")
	for _, d := range []string{"public/sub", "public/empty", "private"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{"public/a.css": "a", "public/sub/index.html": "i", "private/secret": "s"}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// symlinks escaping the mount must not be served, directly or as the
	// directory index
	if err := os.Symlink(filepath.Join(root, "private", "secret"), filepath.Join(mount, "link")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(mount, "evil"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "private", "secret"), filepath.Join(mount, "evil", "index.html")); err != nil {
		t.Fatal(err)
	}
	resolved, _ := filepath.EvalSymlinks(mount)
	cfg := &config.Config{
		StaticMounts:    []config.StaticMount{{Route: "/static", Path: resolved}},
		StaticDirToFile: dirToFile,
		StaticExpires:   60,
	}
	return New(cfg), resolved
}

func TestMatch(t *testing.T) {
	f, mount := newFiles(t, "")
	cases := []struct {
		path string
		file string
		res  Result
	}{
		{"/static/a.css", filepath.Join(mount, "a.css"), Found},
		{"/static/%61.css", filepath.Join(mount, "a.css"), Found},
		{"/static/missing", "", NotFound},
		{"/static/sub", "", NotFound},
		{"/static/../private/secret", "", NotFound},
		{"/static/%2e%2e/private/secret", "", NotFound},
		{"/static/link", "", NotFound},
		{"/static/a.css/x", "", NotFound}, // ENOTDIR: still the mount's
		{"/static/evil", "", NotFound},
		{"/staticfoo/a.css", "", NoMatch},
		{"/other", "", NoMatch},
	}
	for _, c := range cases {
		file, res := f.Match(c.path)
		if res != c.res || file != c.file {
			t.Errorf("%s: got (%q, %v), want (%q, %v)", c.path, file, res, c.file, c.res)
		}
	}
}

func TestMatchDirToFile(t *testing.T) {
	f, mount := newFiles(t, "index.html")
	if file, res := f.Match("/static/sub"); res != Found || file != filepath.Join(mount, "sub", "index.html") {
		t.Errorf("got (%q, %v)", file, res)
	}
	if _, res := f.Match("/static/evil"); res != NotFound {
		t.Error("a directory index linking outside the mount must not be served")
	}
	if _, res := f.Match("/static/empty"); res == NoMatch {
		t.Error("a directory without the index file must not fall through to the application")
	}
}

func TestServe(t *testing.T) {
	f, mount := newFiles(t, "")
	rec := httptest.NewRecorder()
	if !f.Serve(rec, filepath.Join(mount, "a.css")) {
		t.Fatal("not served")
	}
	h := rec.Result().Header
	if rec.Code != 200 || rec.Body.String() != "a" || h.Get("Content-Type") != "text/css" ||
		h.Get("Cache-Control") != "max-age=60" || h.Get("Server") != ServerName {
		t.Errorf("unexpected response: %d %q %v", rec.Code, rec.Body.String(), h)
	}
}

func TestPercentDecode(t *testing.T) {
	for in, want := range map[string]string{
		"/plain": "/plain", "/a%20b": "/a b", "/%E3%81%93": "/こ", "/bad%zz": "/bad%zz", "/end%2": "/end%2", "/%c0": "/\xc0",
	} {
		if got := PercentDecode(in); got != want {
			t.Errorf("%q: got %q, want %q", in, got, want)
		}
	}
}
