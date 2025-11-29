#!/bin/bash
set -e

echo "==================================="
echo "Chaos Engineering Test"
echo "==================================="

echo "Test Scenario: Process restart during replication"
echo ""

# Setup
echo "Setting up test table..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db <<EOF
DROP TABLE IF EXISTS chaos_test CASCADE;
CREATE TABLE chaos_test (
    id SERIAL PRIMARY KEY,
    phase TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE chaos_test REPLICA IDENTITY FULL;
EOF

docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db <<EOF
DROP TABLE IF EXISTS chaos_test;
CREATE TABLE chaos_test (
    id INT PRIMARY KEY,
    phase TEXT,
    created_at TIMESTAMPTZ
);
EOF

# Setup ClickHouse table
echo "Setting up ClickHouse table..."
docker exec -i replicator-clickhouse-1 clickhouse-client -u user --password password --query "DROP TABLE IF EXISTS analytics.chaos_test"
docker exec -i replicator-clickhouse-1 clickhouse-client -u user --password password --query "CREATE TABLE IF NOT EXISTS analytics.chaos_test (id Int32, phase String, created_at DateTime, _version UInt64) ENGINE = ReplacingMergeTree(_version) ORDER BY id"

# Flush Redis to start clean
echo "Flushing Redis..."
docker exec -i replicator-redis-1 redis-cli FLUSHALL > /dev/null

# Ensure replicator is running initially
if ! pgrep -f "bin/replicator" > /dev/null; then
    echo "Starting replicator..."
    ./bin/replicator -config config.yaml > /tmp/replicator_chaos.log 2>&1 &
    sleep 2
fi

# Phase 1: Insert before crash
echo "Phase 1: Inserting 1000 rows..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO chaos_test (phase)
SELECT 'before_crash'
FROM generate_series(1, 1000);
" > /dev/null

sleep 2

# Get replicator PID
REPLICATOR_PID=$(pgrep -f "bin/replicator" || echo "")

if [ -n "$REPLICATOR_PID" ]; then
    echo "Found replicator process: $REPLICATOR_PID"
    echo "Killing replicator..."
    kill -9 $REPLICATOR_PID
    sleep 1
else
    echo "Replicator not running? This shouldn't happen."
    exit 1
fi

# Phase 2: Insert during downtime
echo "Phase 2: Inserting 1000 rows while replicator is down..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO chaos_test (phase)
SELECT 'during_downtime'
FROM generate_series(1, 1000);
" > /dev/null

echo "Restarting replicator..."
./bin/replicator -config config.yaml >> /tmp/replicator_chaos.log 2>&1 &
echo "Waiting 10 seconds for restart and catch-up..."
sleep 10

# Phase 3: Insert after recovery
echo "Phase 3: Inserting 1000 rows after recovery..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO chaos_test (phase)
SELECT 'after_recovery'
FROM generate_series(1, 1000);
" > /dev/null

echo "Waiting for final replication..."
sleep 5

# Verify
echo ""
echo "Verification:"
SOURCE_COUNT=$(docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -t -c "SELECT COUNT(*) FROM chaos_test;")
PG_SINK_COUNT=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "SELECT COUNT(*) FROM chaos_test;")
CH_SINK_COUNT=$(docker exec -i replicator-clickhouse-1 clickhouse-client -u user --password password --query "SELECT count(*) FROM analytics.chaos_test FINAL" 2>/dev/null || echo "0")
REDIS_COUNT=$(docker exec -i replicator-redis-1 redis-cli KEYS "chaos_test:*" 2>/dev/null | wc -l | xargs)

SOURCE_COUNT=$(echo $SOURCE_COUNT | xargs)
PG_SINK_COUNT=$(echo $PG_SINK_COUNT | xargs)
CH_SINK_COUNT=$(echo $CH_SINK_COUNT | xargs)

echo "Source total:       $SOURCE_COUNT"
echo "PostgreSQL sink:    $PG_SINK_COUNT"
echo "ClickHouse sink:    $CH_SINK_COUNT"
echo "Redis keys:         $REDIS_COUNT"
echo ""

echo "PostgreSQL sink breakdown:"
docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -c "
SELECT phase, COUNT(*) as count
FROM chaos_test
GROUP BY phase
ORDER BY phase;
"

echo ""
echo "ClickHouse sink breakdown:"
docker exec -i replicator-clickhouse-1 clickhouse-client -u user --password password --query "
SELECT phase, count(*) as count
FROM analytics.chaos_test
GROUP BY phase
ORDER BY phase
FORMAT PrettyCompact
" 2>/dev/null || echo "ClickHouse query failed"

if [ "$SOURCE_COUNT" -eq "$PG_SINK_COUNT" ] && [ "$SOURCE_COUNT" -eq "$CH_SINK_COUNT" ] && [ "$SOURCE_COUNT" -eq "3000" ]; then
    echo ""
    echo "✅ SUCCESS: All data replicated correctly to all sinks after crash!"
else
    echo ""
    echo "❌ FAILURE: Data loss detected!"
    echo "   Expected: 3000 rows in all sinks"
    echo "   PostgreSQL: $PG_SINK_COUNT"
    echo "   ClickHouse: $CH_SINK_COUNT"
    exit 1
fi
