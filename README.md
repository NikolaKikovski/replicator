# Replicator - High-Availability WAL Replication Service

A production-grade PostgreSQL WAL replication service written in Go that streams changes to multiple downstream targets (PostgreSQL, ClickHouse) with fault tolerance and high throughput.

## Features

- 🚀 **High Performance**: 20k-50k events/sec throughput
- 🔄 **Multi-Target**: Replicate to multiple PostgreSQL and ClickHouse instances simultaneously
- 🛡️ **Fault Tolerant**: Automatic crash recovery with checkpoint management
- 🔁 **Retry Logic**: Configurable exponential backoff per target
- 📊 **Observability**: Prometheus metrics and structured logging
- ⚙️ **Flexible Configuration**: YAML-based configuration with validation

## Architecture

```
PostgreSQL (WAL) → Ingestor → Decoder → Dispatcher → Workers → BroadcastSink
                       ↓                                              ↓
                  CheckpointMgr                              [Postgres, ClickHouse...]
```

## Quick Start

### 1. Start Infrastructure

```bash
docker-compose up -d
```

### 2. Configure

Create `config.yaml`:

```yaml
source:
  connection_string: "postgres://postgres:password@localhost:5432/source_db?replication=database"
  slot_name: "replicator_slot"
  publication: "my_pub"

targets:
  postgres:
    - name: "pg_main"
      connection_string: "postgres://postgres:password@localhost:5433/sink_db"
      batch_size: 1000
      batch_interval: 1s
      retry:
        max_attempts: 5
        backoff: 200ms

  clickhouse:
    - name: "ch_analytics"
      connection_string: "clickhouse://localhost:9000"
      batch_size: 10000
      batch_interval: 5s
      retry:
        max_attempts: 3
        backoff: 500ms

pipeline:
  worker_count: 8
  buffer_size: 20000
  batch_size: 1000
  batch_interval: 1s

telemetry:
  address: ":9090"
```

### 3. Setup Source Database

```sql
-- Create publication
CREATE PUBLICATION my_pub FOR ALL TABLES;

-- Create replication slot
SELECT pg_create_logical_replication_slot('replicator_slot', 'pgoutput');

-- Set replica identity on tables
ALTER TABLE my_table REPLICA IDENTITY FULL;
```

### 4. Run Replicator

```bash
./bin/replicator -config config.yaml
```

## Building

```bash
make build
```

## Testing

### Unit Tests
```bash
go test -v ./...
```

### Stress Test
```bash
./scripts/stress_test.sh 100000 1000
```

### Chaos Test (Crash Recovery)
```bash
./scripts/chaos_test.sh
```

See [TESTING.md](TESTING.md) for detailed testing documentation.

## Configuration Reference

### Source

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `connection_string` | string | Yes | PostgreSQL connection string with `replication=database` |
| `slot_name` | string | Yes | Name of the replication slot |
| `publication` | string | No | Publication name (default: all tables) |

### Target (Postgres/ClickHouse)

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `name` | string | Yes | - | Unique target identifier |
| `connection_string` | string | Yes | - | Target database connection string |
| `batch_size` | int | No | 1000 (PG), 5000 (CH) | Max rows per batch |
| `batch_interval` | duration | No | 1s (PG), 2s (CH) | Max time between flushes |
| `retry.max_attempts` | int | No | 3 | Max retry attempts |
| `retry.backoff` | duration | No | 100ms | Initial backoff duration |

### Pipeline

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `worker_count` | int | 4 | Number of worker goroutines |
| `buffer_size` | int | 10000 | Internal event buffer size |
| `batch_size` | int | 1000 | Worker batch size |
| `batch_interval` | duration | 1s | Worker flush interval |

## Metrics

Available at `:9090/metrics`:

- `replicator_events_processed_total`: Events processed counter
- `replicator_batch_size`: Batch size histogram
- `replicator_sink_latency_seconds`: Sink operation latency
- `replicator_lag_bytes`: Replication lag in bytes

## Design Documents

- [design_doc.md](design_doc.md) - System architecture and design
- [TESTING.md](TESTING.md) - Testing guide

## Operational Considerations

### Performance Tuning

1. **Worker Count**: Increase for higher parallelism (4-16)
2. **Batch Size**: Larger batches = higher throughput but more memory
3. **Buffer Size**: Should be > `batch_size * worker_count`

### Monitoring

Watch these metrics:
- `replicator_lag_bytes` - Should stay low
- `replicator_sink_latency_seconds` - Check for slow sinks
- CPU/Memory usage - Should be stable

### Troubleshooting

**Replication lag growing:**
- Increase `worker_count`
- Increase `batch_size`
- Check sink performance

**High memory usage:**
- Decrease `buffer_size`
- Decrease `batch_size`

**Connection errors:**
- Verify PostgreSQL allows replication connections
- Check `pg_hba.conf` for proper permissions
- Verify replication slot exists

## License

MIT
