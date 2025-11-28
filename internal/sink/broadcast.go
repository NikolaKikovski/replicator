package sink

import (
	"context"
	"fmt"
	"sync"

	"github.com/nikolay-makurin/replicator/pkg/types"
)

// BroadcastSink writes to multiple sinks in parallel.
// It returns an error if ANY sink fails.
type BroadcastSink struct {
	sinks []Sink
}

func NewBroadcastSink(sinks []Sink) *BroadcastSink {
	return &BroadcastSink{sinks: sinks}
}

func (b *BroadcastSink) Write(ctx context.Context, batch *types.Batch) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(b.sinks))

	for _, s := range b.sinks {
		wg.Add(1)
		go func(s Sink) {
			defer wg.Done()
			if err := s.Write(ctx, batch); err != nil {
				errCh <- err
			}
		}(s)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		// Return the first error, or a combined one.
		// For simplicity, just format them all.
		return fmt.Errorf("broadcast failed: %v", errs)
	}

	return nil
}

func (b *BroadcastSink) Close() error {
	var errs []error
	for _, s := range b.sinks {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("close failed: %v", errs)
	}
	return nil
}
