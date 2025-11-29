package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"text/template"
	"time"

	"github.com/nikolay-makurin/replicator/internal/config"
	"github.com/nikolay-makurin/replicator/pkg/types"
	"github.com/redis/go-redis/v9"
)

type RedisSink struct {
	client     *redis.Client
	keyTmpl    *template.Template
	expiration time.Duration
}

func NewRedisSink(cfg config.RedisTarget) (*RedisSink, error) {
	opt, err := redis.ParseURL(cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid redis connection string: %w", err)
	}

	client := redis.NewClient(opt)

	// Parse key pattern template
	tmpl, err := template.New("key").Parse(cfg.KeyPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid key pattern: %w", err)
	}

	return &RedisSink{
		client:     client,
		keyTmpl:    tmpl,
		expiration: 0, // No expiration by default
	}, nil
}

func (s *RedisSink) Write(ctx context.Context, batch *types.Batch) error {
	slog.Info("RedisSink received batch", "count", len(batch.Events))
	pipe := s.client.Pipeline()

	for _, e := range batch.Events {
		// Only handle INSERT and UPDATE for now (SET)
		// DELETE could be DEL
		if e.Type == types.EventDelete {
		// Build template data including table name
		templateData := make(map[string]interface{})
		for k, v := range e.Identity {
			templateData[k] = v
		}
		templateData["table"] = e.Table
		templateData["schema"] = e.Schema

		key, err := s.generateKey(templateData)
		if err != nil {
			return err
		}
		pipe.Del(ctx, key)
		continue
	}

		// For Insert/Update, we store the whole row as JSON
		// Build template data including table name
		templateData := make(map[string]interface{})
		for k, v := range e.Columns {
			templateData[k] = v
		}
		templateData["table"] = e.Table
		templateData["schema"] = e.Schema

		key, err := s.generateKey(templateData)
		if err != nil {
			return err
		}

		data, err := json.Marshal(e.Columns)
		if err != nil {
			return fmt.Errorf("failed to marshal event data: %w", err)
		}

		pipe.Set(ctx, key, data, s.expiration)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis pipeline failed: %w", err)
	}

	return nil
}

func (s *RedisSink) Close() error {
	return s.client.Close()
}

func (s *RedisSink) generateKey(data map[string]interface{}) (string, error) {
	var buf bytes.Buffer
	if err := s.keyTmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute key template: %w", err)
	}
	return buf.String(), nil
}
