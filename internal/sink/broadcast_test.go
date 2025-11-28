package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/nikolay-makurin/replicator/pkg/types"
)

type mockSink struct {
	writeFunc func(ctx context.Context, batch *types.Batch) error
	closeCalled bool
}

func (m *mockSink) Write(ctx context.Context, batch *types.Batch) error {
	if m.writeFunc != nil {
		return m.writeFunc(ctx, batch)
	}
	return nil
}

func (m *mockSink) Close() error {
	m.closeCalled = true
	return nil
}

func TestBroadcastSink(t *testing.T) {
	t.Run("successful broadcast to all sinks", func(t *testing.T) {
		var sink1Called, sink2Called bool
		
		sink1 := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				sink1Called = true
				return nil
			},
		}
		sink2 := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				sink2Called = true
				return nil
			},
		}

		bs := NewBroadcastSink([]Sink{sink1, sink2})
		batch := &types.Batch{
			Events: []*types.Event{
				{Type: types.EventInsert, Table: "users"},
			},
		}

		err := bs.Write(context.Background(), batch)
		if err != nil {
			t.Errorf("Expected no error, got: %v", err)
		}
		if !sink1Called {
			t.Error("Sink1 was not called")
		}
		if !sink2Called {
			t.Error("Sink2 was not called")
		}
	})

	t.Run("error if any sink fails", func(t *testing.T) {
		sink1 := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				return nil
			},
		}
		sink2 := &mockSink{
			writeFunc: func(ctx context.Context, batch *types.Batch) error {
				return errors.New("sink2 failed")
			},
		}

		bs := NewBroadcastSink([]Sink{sink1, sink2})
		batch := &types.Batch{}

		err := bs.Write(context.Background(), batch)
		if err == nil {
			t.Error("Expected error when sink fails, got nil")
		}
	})

	t.Run("close all sinks", func(t *testing.T) {
		sink1 := &mockSink{}
		sink2 := &mockSink{}

		bs := NewBroadcastSink([]Sink{sink1, sink2})
		bs.Close()

		if !sink1.closeCalled {
			t.Error("Sink1 Close was not called")
		}
		if !sink2.closeCalled {
			t.Error("Sink2 Close was not called")
		}
	})
}
