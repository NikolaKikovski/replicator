package sink

import (
	"context"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

type Sink interface {
	Write(ctx context.Context, batch *types.Batch) error
	Close() error
}
