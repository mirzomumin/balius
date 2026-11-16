package metrics

import (
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	a := NewAggregator(2)
	a.Spawns.Add(2)
	w := NewWorker()
	w.ReqHandled.Add(7)
	w.ConnActive.Add(3)
	a.Collect(1, w.Snapshot())
	a.Collect(5, w.Snapshot()) // out of range: ignored

	out := a.Render()
	for _, want := range []string{
		"# TYPE tulpar_workers_spawns counter\ntulpar_workers_spawns 2\n",
		"# TYPE tulpar_connections_active gauge\ntulpar_connections_active{worker=\"2\"} 3\n",
		"tulpar_requests_handled{worker=\"2\"} 7\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, `worker="1"`) {
		t.Error("a worker that never reported must not be rendered")
	}
}
