package sink

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

func TestRetrySink(t *testing.T) {
	t.Run("success on first attempt", func(t *testing.T) {
		var attempts int
		mock := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				attempts++
				return nil
			},
		}

		rs := NewRetrySink("test", mock, config.RetryConfig{
			MaxAttempts: 3,
			Backoff:     10 * time.Millisecond,
		})

		err := rs.Write(context.Background(), &types.Batch{})
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if attempts != 1 {
			t.Errorf("Expected 1 attempt, got %d", attempts)
		}
	})

	t.Run("retry on failure then succeed", func(t *testing.T) {
		var attempts int
		mock := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				attempts++
				if attempts < 3 {
					return errors.New("temporary failure")
				}
				return nil
			},
		}

		rs := NewRetrySink("test", mock, config.RetryConfig{
			MaxAttempts: 5,
			Backoff:     10 * time.Millisecond,
		})

		start := time.Now()
		err := rs.Write(context.Background(), &types.Batch{})
		duration := time.Since(start)

		if err != nil {
			t.Errorf("Expected success after retries, got: %v", err)
		}
		if attempts != 3 {
			t.Errorf("Expected 3 attempts, got %d", attempts)
		}
		// Should have backoff delay
		if duration < 20*time.Millisecond {
			t.Error("Expected backoff delay")
		}
	})

	t.Run("fail after max attempts", func(t *testing.T) {
		var attempts int
		mock := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				attempts++
				return errors.New("persistent failure")
			},
		}

		rs := NewRetrySink("test", mock, config.RetryConfig{
			MaxAttempts: 3,
			Backoff:     10 * time.Millisecond,
		})

		err := rs.Write(context.Background(), &types.Batch{})
		if err == nil {
			t.Error("Expected error after max attempts")
		}
		if attempts != 3 {
			t.Errorf("Expected 3 attempts, got %d", attempts)
		}
	})

	t.Run("respect context cancellation", func(t *testing.T) {
		mock := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				return errors.New("failure")
			},
		}

		rs := NewRetrySink("test", mock, config.RetryConfig{
			MaxAttempts: 10,
			Backoff:     100 * time.Millisecond,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := rs.Write(ctx, &types.Batch{})
		if err == nil {
			t.Error("Expected context error")
		}
	})
}
