package sink

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/pkg/types"
)

type PostgresSink struct {
	pool *pgxpool.Pool
}

func NewPostgresSink(ctx context.Context, cfg config.PostgresTarget) (*PostgresSink, error) {
	pool, err := pgxpool.New(ctx, cfg.ConnectionString)
	if err != nil {
		return nil, err
	}
	return &PostgresSink{pool: pool}, nil
}

func (s *PostgresSink) Write(ctx context.Context, batch *types.Batch) error {
	pgBatch := &pgx.Batch{}

	for _, e := range batch.Events {
		switch e.Type {
		case types.EventInsert:
			cols, vals := mapToSlice(e.Columns)
			placeholders := make([]string, len(cols))
			for i := range placeholders {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
			}
			query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING", 
				e.Schema, e.Table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))
			pgBatch.Queue(query, vals...)

		case types.EventUpdate:
			// UPDATE table SET c1=$1 WHERE pk=$2
			// Requires knowing PK. e.Identity contains PK.
			setCols, setVals := mapToSlice(e.Columns)
			pkCols, pkVals := mapToSlice(e.Identity)
			
			if len(pkCols) == 0 {
				continue // Cannot update without identity
			}

			setParts := make([]string, len(setCols))
			for i, col := range setCols {
				setParts[i] = fmt.Sprintf("%s = $%d", col, i+1)
			}
			
			whereParts := make([]string, len(pkCols))
			for i, col := range pkCols {
				whereParts[i] = fmt.Sprintf("%s = $%d", col, len(setCols)+i+1)
			}

			query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
				e.Schema, e.Table, strings.Join(setParts, ", "), strings.Join(whereParts, " AND "))
			
			args := append(setVals, pkVals...)
			pgBatch.Queue(query, args...)

		case types.EventDelete:
			pkCols, pkVals := mapToSlice(e.Identity)
			if len(pkCols) == 0 {
				continue
			}
			whereParts := make([]string, len(pkCols))
			for i, col := range pkCols {
				whereParts[i] = fmt.Sprintf("%s = $%d", col, i+1)
			}
			query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
				e.Schema, e.Table, strings.Join(whereParts, " AND "))
			pgBatch.Queue(query, pkVals...)
		}
	}

	br := s.pool.SendBatch(ctx, pgBatch)
	defer br.Close()

	for i := 0; i < pgBatch.Len(); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("batch execution failed at index %d: %w", i, err)
		}
	}

	return nil
}

func (s *PostgresSink) Close() error {
	s.pool.Close()
	return nil
}

func mapToSlice(m map[string]interface{}) ([]string, []interface{}) {
	cols := make([]string, 0, len(m))
	vals := make([]interface{}, 0, len(m))
	for k, v := range m {
		cols = append(cols, k)
		vals = append(vals, v)
	}
	return cols, vals
}
