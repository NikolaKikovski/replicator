package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/internal/pipeline"
	"github.com/nikolay-makurin/replicator/internal/sink"
	"github.com/nikolay-makurin/replicator/internal/source/postgres"
	"github.com/nikolay-makurin/replicator/internal/telemetry"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

func main() {
	configPath := flag.String("config", "", "Path to config file")
	flag.Parse()

	// 1. Config
	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	// 2. Telemetry
	telemetry.Init(cfg.Telemetry.Address)
	slog.Info("Starting Replicator", "source_slot", cfg.Source.SlotName)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Checkpoint Manager
	cm := pipeline.NewCheckpointManager(0)

	// 4. Sinks
	var sinks []sink.Sink

	// Initialize Postgres Sinks
	for _, t := range cfg.Targets.Postgres {
		s, err := sink.NewPostgresSink(ctx, t)
		if err != nil {
			slog.Error("Failed to init postgres sink", "name", t.Name, "error", err)
			os.Exit(1)
		}
		// Wrap with Retry
		rs := sink.NewRetrySink(t.Name, s, t.Retry)
		sinks = append(sinks, rs)
		slog.Info("Initialized Postgres sink", "name", t.Name)
	}

	// Initialize ClickHouse Sinks
	for _, t := range cfg.Targets.ClickHouse {
		s, err := sink.NewClickHouseSink(t)
		if err != nil {
			slog.Error("Failed to init clickhouse sink", "name", t.Name, "error", err)
			os.Exit(1)
		}
		// Wrap with Retry
		rs := sink.NewRetrySink(t.Name, s, t.Retry)
		sinks = append(sinks, rs)
		slog.Info("Initialized ClickHouse sink", "name", t.Name)
	}

	// Initialize Redis Sinks
	for _, t := range cfg.Targets.Redis {
		s, err := sink.NewRedisSink(t)
		if err != nil {
			slog.Error("Failed to init redis sink", "name", t.Name, "error", err)
			os.Exit(1)
		}
		// Wrap with Retry
		rs := sink.NewRetrySink(t.Name, s, t.Retry)
		sinks = append(sinks, rs)
		slog.Info("Initialized Redis sink", "name", t.Name)
	}

	if len(sinks) == 0 {
		slog.Error("No sinks configured")
		os.Exit(1)
	}

	// Wrap in Broadcast
	broadcastSink := sink.NewBroadcastSink(sinks)
	defer broadcastSink.Close()

	// 5. Dispatcher
	dispatcher := pipeline.NewDispatcher(cfg.Pipeline, broadcastSink, cm)
	eventCh := make(chan *types.Event, cfg.Pipeline.BufferSize)

	// 6. Source
	src := postgres.NewSource(cfg.Source, cm, eventCh)

	// 7. Start Components
	go dispatcher.Start(ctx, eventCh)

	// Start source (will block in main goroutine until shutdown)
	go func() {
		if err := src.Start(ctx); err != nil {
			slog.Error("Source failed", "error", err)
			cancel()
		}
	}()

	// 8. Wait for Signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down...")
	cancel()
	// Give goroutines time to clean up
	time.Sleep(2 * time.Second)
}
