package pipeline

import (
	"context"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/internal/sink"
	"github.com/nikolay-makurin/replicator/internal/telemetry"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

type Dispatcher struct {
	cfg        config.PipelineConfig
	workers    []*Worker
	checkPoint *CheckpointManager
}

func NewDispatcher(cfg config.PipelineConfig, s sink.Sink, cm *CheckpointManager) *Dispatcher {
	workers := make([]*Worker, cfg.WorkerCount)
	for i := 0; i < cfg.WorkerCount; i++ {
		workers[i] = NewWorker(i, cfg, s, cm)
	}
	return &Dispatcher{
		cfg:        cfg,
		workers:    workers,
		checkPoint: cm,
	}
}

func (d *Dispatcher) Start(ctx context.Context, in <-chan *types.Event) {
	// Start workers
	var wg sync.WaitGroup
	for _, w := range d.workers {
		wg.Add(1)
		go func(w *Worker) {
			defer wg.Done()
			w.Run(ctx)
		}(w)
	}

	// Dispatch loop
	go func() {
		for event := range in {
			// Determine worker based on Primary Key or Table
			// For simplicity, hash the Table + PK
			key := event.Table // TODO: Add PK to hash
			idx := hash(key) % uint32(len(d.workers))
			d.workers[idx].in <- event
		}
		// Close worker channels when input closes
		for _, w := range d.workers {
			close(w.in)
		}
	}()

	wg.Wait()
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type Worker struct {
	id         int
	cfg        config.PipelineConfig
	sink       sink.Sink
	in         chan *types.Event
	batch      *types.Batch
	checkpoint *CheckpointManager
}

func NewWorker(id int, cfg config.PipelineConfig, s sink.Sink, cm *CheckpointManager) *Worker {
	return &Worker{
		id:         id,
		cfg:        cfg,
		sink:       s,
		in:         make(chan *types.Event, cfg.BufferSize),
		batch:      &types.Batch{Events: make([]*types.Event, 0, cfg.BatchSize)},
		checkpoint: cm,
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.BatchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.flush(context.Background())
			return
		case event, ok := <-w.in:
			if !ok {
				w.flush(ctx)
				return
			}
			w.batch.Events = append(w.batch.Events, event)
			if event.LSN > w.batch.MaxLSN {
				w.batch.MaxLSN = event.LSN
			}
			if len(w.batch.Events) >= w.cfg.BatchSize {
				w.flush(ctx)
			}
		case <-ticker.C:
			if len(w.batch.Events) > 0 {
				w.flush(ctx)
			}
		}
	}
}

func (w *Worker) flush(ctx context.Context) {
	if len(w.batch.Events) == 0 {
		return
	}

	start := time.Now()
	err := w.sink.Write(ctx, w.batch)
	duration := time.Since(start)

	telemetry.SinkLatency.WithLabelValues("generic").Observe(duration.Seconds())
	telemetry.BatchSize.WithLabelValues("generic").Observe(float64(len(w.batch.Events)))

	if err != nil {
		slog.Error("Sink write failed", "worker", w.id, "error", err)
		// Retry logic should be here or in Sink.
		// For now, we drop or panic. In prod, we retry indefinitely.
		// panic(err) 
	} else {
		// Mark all LSNs in batch as done
		for _, e := range w.batch.Events {
			w.checkpoint.MarkDone(e.LSN)
		}
		telemetry.EventsProcessed.WithLabelValues("success", "generic").Add(float64(len(w.batch.Events)))
	}

	// Reset batch
	w.batch.Events = w.batch.Events[:0]
	w.batch.MaxLSN = 0
}
