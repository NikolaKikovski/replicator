package sink

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

type RetrySink struct {
	next Sink
	cfg  config.RetryConfig
	name string
}

func NewRetrySink(name string, next Sink, cfg config.RetryConfig) *RetrySink {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 100 * time.Millisecond
	}
	return &RetrySink{
		next: next,
		cfg:  cfg,
		name: name,
	}
}

func (r *RetrySink) Write(ctx context.Context, batch *types.Batch) error {
	var err error
	for i := 0; i < r.cfg.MaxAttempts; i++ {
		if err = r.next.Write(ctx, batch); err == nil {
			return nil
		}
		
		slog.Warn("Sink write failed, retrying", 
			"sink", r.name, 
			"attempt", i+1, 
			"max_attempts", r.cfg.MaxAttempts, 
			"error", err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.cfg.Backoff * time.Duration(1<<i)): // Exponential backoff
		}
	}
	return fmt.Errorf("sink %s failed after %d attempts: %w", r.name, r.cfg.MaxAttempts, err)
}

func (r *RetrySink) Close() error {
	return r.next.Close()
}
