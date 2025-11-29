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
	db   string // ClickHouse database name
}

func NewClickHouseSink(cfg config.ClickHouseTarget) (*ClickHouseSink, error) {
	opts, err := clickhouse.ParseDSN(cfg.ConnectionString)
	if err != nil {
		return nil, err
	}
	
	// Extract database name from auth.Database (e.g., "analytics")
	dbName := opts.Auth.Database
	if dbName == "" {
		dbName = "default"
	}
	
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &ClickHouseSink{conn: conn, db: dbName}, nil
}

func (s *ClickHouseSink) Write(ctx context.Context, batch *types.Batch) error {
	// Group events by table and type
	type tableKey struct {
		schema string
		table  string
	}
	eventsByTable := make(map[tableKey]map[types.EventType][]*types.Event)

	for _, e := range batch.Events {
		key := tableKey{schema: e.Schema, table: e.Table}
		if eventsByTable[key] == nil {
			eventsByTable[key] = make(map[types.EventType][]*types.Event)
		}
		eventsByTable[key][e.Type] = append(eventsByTable[key][e.Type], e)
	}

	for key, typeMap := range eventsByTable {
		// Use ClickHouse database instead of PostgreSQL schema
		tableName := fmt.Sprintf("%s.%s", s.db, key.table)

		// Handle DELETEs using lightweight DELETE
		if deletes, ok := typeMap[types.EventDelete]; ok && len(deletes) > 0 {
			for _, e := range deletes {
				// Build WHERE clause from identity columns
				whereParts := []string{}
				for col := range e.Identity {
					whereParts = append(whereParts, fmt.Sprintf("%s = ?", col))
				}
				if len(whereParts) == 0 {
					continue // Skip if no identity
				}

				whereClause := strings.Join(whereParts, " AND ")
				query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, whereClause)

				// Extract values in consistent order
				vals := make([]interface{}, 0, len(e.Identity))
				for _, part := range whereParts {
					col := strings.Split(part, " ")[0]
					vals = append(vals, e.Identity[col])
				}

				if err := s.conn.Exec(ctx, query, vals...); err != nil {
					return fmt.Errorf("delete failed for %s: %w", tableName, err)
				}
			}
		}

		// Handle INSERTs and UPDATEs (both as inserts with LSN as version)
		inserts := append(typeMap[types.EventInsert], typeMap[types.EventUpdate]...)
		if len(inserts) > 0 {
			// Collect all columns (including _version)
			first := inserts[0]
			cols := make([]string, 0, len(first.Columns)+1)
			for k := range first.Columns {
				cols = append(cols, k)
			}
			cols = append(cols, "_version") // Add version column

			query := fmt.Sprintf("INSERT INTO %s (%s)", tableName, strings.Join(cols, ", "))
			chBatch, err := s.conn.PrepareBatch(ctx, query)
			if err != nil {
				return fmt.Errorf("prepare batch failed for %s: %w", tableName, err)
			}

			for _, e := range inserts {
				vals := make([]interface{}, len(cols))
				for i, col := range cols {
					if col == "_version" {
						vals[i] = uint64(e.LSN) // Use LSN as version for deduplication
					} else {
						vals[i] = e.Columns[col]
					}
				}
				if err := chBatch.Append(vals...); err != nil {
					return fmt.Errorf("append failed: %w", err)
				}
			}

			if err := chBatch.Send(); err != nil {
				return fmt.Errorf("batch send failed for %s: %w", tableName, err)
			}
		}
	}
	return nil
}

func (s *ClickHouseSink) Close() error {
	return s.conn.Close()
}
