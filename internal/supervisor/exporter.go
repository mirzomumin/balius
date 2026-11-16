package supervisor

import (
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/mirzomumin/tulpar/internal/metrics"
)

// exporter serves the Prometheus metrics endpoint.
type exporter struct {
	srv *http.Server
}

func startExporter(addr string, port int, agg *metrics.Aggregator) (*exporter, error) {
	l, err := net.Listen("tcp", net.JoinHostPort(addr, strconv.Itoa(port)))
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = io.WriteString(w, agg.Render())
		}),
	}
	go func() { _ = srv.Serve(l) }()
	return &exporter{srv: srv}, nil
}

func (e *exporter) stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = e.srv.Shutdown(ctx)
}
