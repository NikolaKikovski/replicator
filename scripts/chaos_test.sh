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

# Phase 1: Insert before crash
echo "Phase 1: Inserting 1000 rows..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO chaos_test (phase)
SELECT 'before_crash'
FROM generate_series(1, 1000);
" > /dev/null

sleep 2

# Get replicator PID if running
REPLICATOR_PID=$(pgrep -f "bin/replicator" || echo "")

if [ -n "$REPLICATOR_PID" ]; then
    echo "Found replicator process: $REPLICATOR_PID"
    echo "Killing replicator..."
    kill -9 $REPLICATOR_PID
    sleep 1
else
    echo "Replicator not running (manual test mode)"
fi

# Phase 2: Insert during downtime
echo "Phase 2: Inserting 1000 rows while replicator is down..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO chaos_test (phase)
SELECT 'during_downtime'
FROM generate_series(1, 1000);
" > /dev/null

echo "Replicator should now be restarted manually..."
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
SINK_COUNT=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "SELECT COUNT(*) FROM chaos_test;")

SOURCE_COUNT=$(echo $SOURCE_COUNT | xargs)
SINK_COUNT=$(echo $SINK_COUNT | xargs)

echo "Source total: $SOURCE_COUNT"
echo "Sink total:   $SINK_COUNT"

docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -c "
SELECT phase, COUNT(*) as count
FROM chaos_test
GROUP BY phase
ORDER BY phase;
"

if [ "$SOURCE_COUNT" -eq "$SINK_COUNT" ] && [ "$SOURCE_COUNT" -eq "3000" ]; then
    echo ""
    echo "✅ SUCCESS: All data replicated correctly after crash!"
else
    echo ""
    echo "❌ FAILURE: Data loss detected!"
    exit 1
fi
