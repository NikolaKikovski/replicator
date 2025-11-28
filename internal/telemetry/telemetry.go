package telemetry

import (
	"log/slog"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	EventsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "replicator_events_processed_total",
			Help: "The total number of events processed",
		},
		[]string{"status", "sink"},
	)
	BatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "replicator_batch_size",
			Help: "Distribution of batch sizes",
			Buckets: []float64{10, 100, 500, 1000, 5000, 10000},
		},
		[]string{"sink"},
	)
	SinkLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "replicator_sink_latency_seconds",
			Help: "Latency of sink operations",
		},
		[]string{"sink"},
	)
	LagBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "replicator_lag_bytes",
			Help: "Estimated lag in bytes behind the source",
		},
	)
)

func Init(addr string) {
	// Metrics
	prometheus.MustRegister(EventsProcessed)
	prometheus.MustRegister(BatchSize)
	prometheus.MustRegister(SinkLatency)
	prometheus.MustRegister(LagBytes)

	// Logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	// Server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		slog.Info("Starting telemetry server", "address", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			slog.Error("Telemetry server failed", "error", err)
		}
	}()
}
