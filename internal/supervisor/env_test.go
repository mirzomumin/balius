package supervisor

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mirzomumin/balius/internal/config"
)

func TestParseEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	data := `# comment
PLAIN=value
export EXPORTED=yes
SPACED = padded  # trailing comment
SINGLE='keep $raw # text'
DOUBLE="line\nbreak \"quoted\""
EMPTY=
not a line
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := parseEnvFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := [][2]string{
		{"PLAIN", "value"},
		{"EXPORTED", "yes"},
		{"SPACED", "padded"},
		{"SINGLE", "keep $raw # text"},
		{"DOUBLE", "line\nbreak \"quoted\""},
		{"EMPTY", ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestLoadEnvPrecedence(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	_ = os.WriteFile(path, []byte("A=from-file\nB=from-file\n"), 0o644)
	s := &Supervisor{baseEnv: []string{"A=from-env"}}
	s.cfg = config.Default()
	s.cfg.EnvFiles = []string{path}
	if err := s.loadEnv(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.env, []string{"A=from-env", "B=from-file"}) {
		t.Errorf("got %v", s.env)
	}
}

func TestWorkerEnviron(t *testing.T) {
	got := workerEnviron([]string{"PATH=/bin", "BALIUS_SSL_KEYFILE_PASSWORD=x", "BALIUS_CPUPROFILE=/p", "XBALIUS_=1"})
	if !reflect.DeepEqual(got, []string{"PATH=/bin", "BALIUS_CPUPROFILE=/p", "XBALIUS_=1"}) {
		t.Errorf("got %v", got)
	}
}
