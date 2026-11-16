package supervisor

import "testing"

func TestChangeFilter(t *testing.T) {
	f, err := newChangeFilter([]string{"build"}, []string{`\.log$`}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]bool{
		"/src/app.py": true, "/src/app.pyc": false, "/src/__pycache__/x.py": false, "/src/build/x.py": false,
		"/src/out.log": false, "/src/.git/HEAD": false, "/src/main.go": true,
	} {
		if got := f.accepts(path); got != want {
			t.Errorf("%s: got %v, want %v", path, got, want)
		}
	}
}
