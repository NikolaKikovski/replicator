# Quick Start Guide

## First Time Setup

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Run End-to-End Test
This sets up everything and verifies basic replication:
```bash
make e2e-test
```

The e2e test will:
- ✅ Create source and sink databases
- ✅ Setup publication and replication slot
- ✅ Start the replicator in the background
- ✅ Insert test data and verify replication

**The replicator will keep running after this test completes.**

### 3. Run Stress Test
Now that the replicator is running, test with high volume:
```bash
make stress-test
```

## Common Workflow

```bash
# Start containers
docker-compose up -d

# Run e2e test (starts replicator)
make e2e-test

# Run stress test (uses running replicator)
make stress-test

# Check logs
tail -f /tmp/replicator.log

# Stop replicator
pkill -f bin/replicator
```

## Troubleshooting

### "Replicator is not running" Error

If you see:
```
❌ ERROR: Replicator is not running!
```

Run the e2e test first:
```bash
make e2e-test
```

### Check Replicator Status

```bash
# Check if running
pgrep -f bin/replicator

# View logs
tail -f /tmp/replicator.log

# Check metrics
curl localhost:9090/metrics | grep replicator
```

### Manually Start Replicator

If you need to start it manually:
```bash
# Build first
make build

# Start in background
./bin/replicator -config config.yaml > /tmp/replicator.log 2>&1 &

# Or in foreground (to see logs)
./bin/replicator -config config.yaml
```

### Database Not Ready

If you see connection errors:
```bash
# Check containers are up
docker-compose ps

# Check database health
docker exec replicator-pg_source-1 pg_isready -U postgres
docker exec replicator-pg_sink-1 pg_isready -U postgres
```

### Replication Slot Issues

If the replicator can't connect to the slot:
```bash
# Check if slot exists
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c \
  "SELECT * FROM pg_replication_slots WHERE slot_name = 'replicator_slot';"

# If not, run e2e test to create it
make e2e-test
```

## Test Sequence

**Recommended order:**

1. **Unit Tests** (no infrastructure needed)
   ```bash
   make test
   ```

2. **E2E Test** (starts everything)
   ```bash
   make e2e-test
   ```

3. **Stress Test** (requires running replicator from step 2)
   ```bash
   make stress-test
   ```

4. **Chaos Test** (tests crash recovery)
   ```bash
   make chaos-test
   ```
