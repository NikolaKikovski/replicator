# Testing Documentation

## Unit Tests

Run all unit tests:
```bash
go test -v ./...
```

Run with coverage:
```bash
go test -v -race -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Test Suites

1. **Config Tests** (`internal/config/config_test.go`)
   - YAML loading and parsing
   - Validation logic
   - Default values

2. **BroadcastSink Tests** (`internal/sink/broadcast_test.go`)
   - Parallel writes to multiple sinks
   - Error propagation
   - Close behavior

3. **RetrySink Tests** (`internal/sink/retry_test.go`)
   - Exponential backoff
   - Max attempts
   - Context cancellation

4. **CheckpointManager Tests** (`internal/pipeline/checkpoint_test.go`)
   - Out-of-order LSN handling
   - Safe LSN calculation

## Integration Tests

### Prerequisites
```bash
docker-compose up -d
```

### Stress Test

Tests high-volume replication:
```bash
./scripts/stress_test.sh [num_rows] [batch_size]

# Examples:
./scripts/stress_test.sh 100000 1000    # 100k rows, batch 1000
./scripts/stress_test.sh 1000000 5000   # 1M rows, batch 5000
```

**What it tests:**
- Inserts N rows into source database
- Verifies all rows replicate to sink
- Calculates throughput (rows/sec)
- Checks replication lag

### Chaos Test

Tests crash recovery:
```bash
./scripts/chaos_test.sh
```

**What it tests:**
- Inserts data before crash
- Kills replicator process
- Inserts data during downtime
- Verifies replicator catches up after restart
- Validates zero data loss

### Performance Benchmark

Compares different batch sizes:
```bash
./scripts/benchmark.sh
```

**What it tests:**
- Runs stress test with various batch sizes
- Measures throughput for each
- Helps identify optimal configuration

## Manual Testing Checklist

### Basic Replication
- [ ] Start docker-compose
- [ ] Run replicator with config.yaml
- [ ] Insert data in source
- [ ] Verify data appears in sink
- [ ] Check Prometheus metrics at :9090/metrics

### Multi-Target Testing
- [ ] Configure 2+ Postgres targets
- [ ] Verify data replicates to all
- [ ] Stop one target
- [ ] Verify replication stops (error in logs)
- [ ] Restart target
- [ ] Verify catch-up

### Failure Recovery
- [ ] Kill replicator during load
- [ ] Restart
- [ ] Verify resume from checkpoint
- [ ] Verify no duplicates (idempotent writes)

### Performance Testing
- [ ] Run stress test with 100k rows
- [ ] Monitor CPU/memory usage
- [ ] Check replication lag metric
- [ ] Verify throughput > 10k rows/sec

## Expected Metrics

For a typical deployment:
- **Throughput**: 20k-50k events/sec
- **Latency**: < 100ms p99
- **CPU**: < 50% (4 workers)
- **Memory**: < 500MB

## Troubleshooting

### Common Issues

1. **Replication slot not found**
   ```sql
   SELECT * FROM pg_create_logical_replication_slot('replicator_slot', 'pgoutput');
   ```

2. **Publication not found**
   ```sql
   CREATE PUBLICATION my_pub FOR ALL TABLES;
   ```

3. **Replica identity issues**
   ```sql
   ALTER TABLE my_table REPLICA IDENTITY FULL;
   ```

4. **Connection refused**
   - Check `connection_string` in config.yaml
   - Verify PostgreSQL allows replication connections
   - Check `pg_hba.conf` for replication permissions
