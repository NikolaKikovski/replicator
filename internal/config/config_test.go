package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	configContent := `
source:
  connection_string: "postgres://localhost/db"
  slot_name: "test_slot"
  publication: "test_pub"

targets:
  postgres:
    - name: "pg1"
      connection_string: "postgres://localhost/sink1"
      batch_size: 500
      retry:
        max_attempts: 3
        backoff: 100ms
  clickhouse:
    - name: "ch1"
      connection_string: "clickhouse://localhost:9000"

pipeline:
  worker_count: 2
`
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	cfg, err := Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Validate source
	if cfg.Source.SlotName != "test_slot" {
		t.Errorf("Expected slot_name 'test_slot', got %s", cfg.Source.SlotName)
	}

	// Validate targets
	if len(cfg.Targets.Postgres) != 1 {
		t.Errorf("Expected 1 postgres target, got %d", len(cfg.Targets.Postgres))
	}
	if cfg.Targets.Postgres[0].Name != "pg1" {
		t.Errorf("Expected name 'pg1', got %s", cfg.Targets.Postgres[0].Name)
	}
	if cfg.Targets.Postgres[0].BatchSize != 500 {
		t.Errorf("Expected batch_size 500, got %d", cfg.Targets.Postgres[0].BatchSize)
	}

	// Validate ClickHouse target got defaults
	if len(cfg.Targets.ClickHouse) != 1 {
		t.Errorf("Expected 1 clickhouse target, got %d", len(cfg.Targets.ClickHouse))
	}
	if cfg.Targets.ClickHouse[0].BatchSize != 5000 {
		t.Errorf("Expected default batch_size 5000, got %d", cfg.Targets.ClickHouse[0].BatchSize)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid config",
			config: Config{
				Source: SourceConfig{
					ConnectionString: "postgres://localhost/db",
					SlotName:         "slot",
				},
				Targets: TargetsConfig{
					Postgres: []PostgresTarget{
						{
							TargetBase: TargetBase{
								Name:          "pg1",
								BatchSize:     1000,
								BatchInterval: 1 * time.Second,
							},
							ConnectionString: "postgres://localhost/sink",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "missing source connection",
			config: Config{
				Source: SourceConfig{
					SlotName: "slot",
				},
				Targets: TargetsConfig{
					Postgres: []PostgresTarget{
						{
							TargetBase: TargetBase{Name: "pg1"},
							ConnectionString: "postgres://localhost/sink",
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "no targets",
			config: Config{
				Source: SourceConfig{
					ConnectionString: "postgres://localhost/db",
					SlotName:         "slot",
				},
				Targets: TargetsConfig{},
			},
			expectError: true,
		},
		{
			name: "missing target name",
			config: Config{
				Source: SourceConfig{
					ConnectionString: "postgres://localhost/db",
					SlotName:         "slot",
				},
				Targets: TargetsConfig{
					Postgres: []PostgresTarget{
						{
							ConnectionString: "postgres://localhost/sink",
						},
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error, got: %v", err)
			}
		})
	}
}
