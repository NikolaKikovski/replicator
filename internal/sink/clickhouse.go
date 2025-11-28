package sink

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

type ClickHouseSink struct {
	conn driver.Conn
}

func NewClickHouseSink(cfg config.ClickHouseTarget) (*ClickHouseSink, error) {
	opts, err := clickhouse.ParseDSN(cfg.ConnectionString)
	if err != nil {
		return nil, err
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &ClickHouseSink{conn: conn}, nil
}

func (s *ClickHouseSink) Write(ctx context.Context, batch *types.Batch) error {
	// Group by table
	eventsByTable := make(map[string][]*types.Event)
	for _, e := range batch.Events {
		if e.Type == types.EventInsert { // CH mostly for appends
			eventsByTable[e.Table] = append(eventsByTable[e.Table], e)
		}
	}

	for table, events := range eventsByTable {
		if len(events) == 0 {
			continue
		}
		
		// Prepare batch
		// We assume all events for a table have same columns
		first := events[0]
		cols := make([]string, 0, len(first.Columns))
		for k := range first.Columns {
			cols = append(cols, k)
		}

		query := fmt.Sprintf("INSERT INTO %s (%s)", table, strings.Join(cols, ", "))
		chBatch, err := s.conn.PrepareBatch(ctx, query)
		if err != nil {
			return fmt.Errorf("prepare batch failed for %s: %w", table, err)
		}

		for _, e := range events {
			vals := make([]interface{}, len(cols))
			for i, col := range cols {
				vals[i] = e.Columns[col]
			}
			if err := chBatch.Append(vals...); err != nil {
				return err
			}
		}

		if err := chBatch.Send(); err != nil {
			return fmt.Errorf("batch send failed for %s: %w", table, err)
		}
	}
	return nil
}

func (s *ClickHouseSink) Close() error {
	return s.conn.Close()
}
