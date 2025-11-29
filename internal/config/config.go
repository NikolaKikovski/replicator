package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Source    SourceConfig    `mapstructure:"source"`
	Targets   TargetsConfig   `mapstructure:"targets"`
	Pipeline  PipelineConfig  `mapstructure:"pipeline"`
	Telemetry TelemetryConfig `mapstructure:"telemetry"`
}

type SourceConfig struct {
	ConnectionString string `mapstructure:"connection_string"`
	SlotName         string `mapstructure:"slot_name"`
	Publication      string `mapstructure:"publication"`
}

type TargetsConfig struct {
	Postgres   []PostgresTarget   `mapstructure:"postgres"`
	ClickHouse []ClickHouseTarget `mapstructure:"clickhouse"`
	Redis      []RedisTarget      `mapstructure:"redis"`
}

type TargetBase struct {
	Name          string        `mapstructure:"name"`
	BatchSize     int           `mapstructure:"batch_size"`
	BatchInterval time.Duration `mapstructure:"batch_interval"`
	Retry         RetryConfig   `mapstructure:"retry"`
}

type PostgresTarget struct {
	TargetBase       `mapstructure:",squash"`
	ConnectionString string `mapstructure:"connection_string"`
}

type ClickHouseTarget struct {
	TargetBase       `mapstructure:",squash"`
	ConnectionString string `mapstructure:"connection_string"`
}

type RedisTarget struct {
	TargetBase       `mapstructure:",squash"`
	ConnectionString string `mapstructure:"connection_string"`
	KeyPattern       string `mapstructure:"key_pattern"` // e.g. "users:{{.id}}"
}

type RetryConfig struct {
	MaxAttempts int           `mapstructure:"max_attempts"`
	Backoff     time.Duration `mapstructure:"backoff"`
}

func (r *RetryConfig) setDefaults() {
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	}
	if r.Backoff == 0 {
		r.Backoff = 100 * time.Millisecond
	}
}

type PipelineConfig struct {
	WorkerCount   int           `mapstructure:"worker_count"`
	BufferSize    int           `mapstructure:"buffer_size"`
	BatchSize     int           `mapstructure:"batch_size"`
	BatchInterval time.Duration `mapstructure:"batch_interval"`
}

type TelemetryConfig struct {
	Address string `mapstructure:"address"`
}

func Load(configPath string) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("REPLICATOR")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("pipeline.worker_count", 4)
	v.SetDefault("pipeline.buffer_size", 10000)
	v.SetDefault("pipeline.batch_size", 1000)
	v.SetDefault("pipeline.batch_interval", 1*time.Second)
	v.SetDefault("telemetry.address", ":9090")

	// Read config file if provided
	if configPath != "" {
		v.SetConfigFile(configPath)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.setDefaults()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) setDefaults() {
	for i := range c.Targets.Postgres {
		if c.Targets.Postgres[i].BatchSize == 0 {
			c.Targets.Postgres[i].BatchSize = 1000 // Default
		}
		if c.Targets.Postgres[i].BatchInterval == 0 {
			c.Targets.Postgres[i].BatchInterval = 1 * time.Second // Default
		}
		c.Targets.Postgres[i].Retry.setDefaults()
	}

	for i := range c.Targets.ClickHouse {
		if c.Targets.ClickHouse[i].BatchSize == 0 {
			c.Targets.ClickHouse[i].BatchSize = 5000 // Default
		}
		if c.Targets.ClickHouse[i].BatchInterval == 0 {
			c.Targets.ClickHouse[i].BatchInterval = 2 * time.Second // Default
		}
		c.Targets.ClickHouse[i].Retry.setDefaults()
	}

	for i := range c.Targets.Redis {
		if c.Targets.Redis[i].BatchSize == 0 {
			c.Targets.Redis[i].BatchSize = 1000 // Default
		}
		if c.Targets.Redis[i].BatchInterval == 0 {
			c.Targets.Redis[i].BatchInterval = 1 * time.Second // Default
		}
		c.Targets.Redis[i].Retry.setDefaults()
	}
}

func (c *Config) Validate() error {
	if c.Source.ConnectionString == "" {
		return errors.New("source.connection_string is required")
	}
	if c.Source.SlotName == "" {
		return errors.New("source.slot_name is required")
	}
	if len(c.Targets.Postgres) == 0 && len(c.Targets.ClickHouse) == 0 {
		return errors.New("at least one target (postgres or clickhouse) must be defined")
	}

	for i, t := range c.Targets.Postgres {
		if t.Name == "" {
			return fmt.Errorf("targets.postgres[%d].name is required", i)
		}
		if t.ConnectionString == "" {
			return fmt.Errorf("targets.postgres[%d].connection_string is required", i)
		}
		if t.BatchSize <= 0 {
			c.Targets.Postgres[i].BatchSize = 1000 // Default
		}
		if t.BatchInterval <= 0 {
			c.Targets.Postgres[i].BatchInterval = 1 * time.Second // Default
		}
	}

	for i, t := range c.Targets.ClickHouse {
		if t.Name == "" {
			return fmt.Errorf("targets.clickhouse[%d].name is required", i)
		}
		if t.ConnectionString == "" {
			return fmt.Errorf("targets.clickhouse[%d].connection_string is required", i)
		}
		if t.BatchSize <= 0 {
			c.Targets.ClickHouse[i].BatchSize = 5000 // Default
		}
		if t.BatchInterval <= 0 {
			c.Targets.ClickHouse[i].BatchInterval = 2 * time.Second // Default
		}
	}

	return nil
}
