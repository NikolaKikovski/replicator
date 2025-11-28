#!/bin/bash
set -e

echo "==================================="
echo "End-to-End Replication Test"
echo "==================================="
echo ""

# Check if docker-compose is running
if ! docker exec replicator-pg_source-1 pg_isready -U postgres 2>/dev/null; then
    echo "❌ Docker containers not running!"
    echo "   Run: docker-compose up -d"
    exit 1
fi

echo "✅ Docker containers are running"
echo ""

# Setup test table and publication
echo "Step 1: Setting up source database..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db <<EOF
-- Create test table
DROP TABLE IF EXISTS demo CASCADE;
CREATE TABLE demo (
    id SERIAL PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Set replica identity
ALTER TABLE demo REPLICA IDENTITY FULL;

-- Create publication if not exists
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'my_pub') THEN
        CREATE PUBLICATION my_pub FOR ALL TABLES;
    END IF;
END \$\$;

-- Create replication slot if not exists
SELECT CASE 
    WHEN EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replicator_slot')
    THEN 'Slot already exists'
    ELSE (SELECT 'Slot created: ' || slot_name FROM pg_create_logical_replication_slot('replicator_slot', 'pgoutput'))
END;
EOF

echo "✅ Source database configured"
echo ""

# Setup sink table
echo "Step 2: Setting up sink database..."
docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db <<EOF
DROP TABLE IF EXISTS demo;
CREATE TABLE demo (
    id INT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMPTZ
);
EOF

echo "✅ Sink database configured"
echo ""

# Start replicator in background
echo "Step 3: Starting replicator..."
if pgrep -f "bin/replicator" > /dev/null; then
    echo "⚠️  Replicator already running, killing..."
    pkill -f "bin/replicator"
    sleep 1
fi

./bin/replicator -config config.yaml > /tmp/replicator.log 2>&1 &
REPLICATOR_PID=$!
echo "✅ Replicator started (PID: $REPLICATOR_PID)"
echo "   Logs: tail -f /tmp/replicator.log"
echo ""

# Wait for startup
echo "Waiting for replicator to connect..."
sleep 3

# Insert test data
echo "Step 4: Inserting test data..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO demo (name) VALUES 
    ('Alice'),
    ('Bob'),
    ('Charlie'),
    ('David'),
    ('Eve');
" > /dev/null

echo "✅ Inserted 5 rows into source"
echo ""

# Wait for replication
echo "Step 5: Waiting for replication (5 seconds)..."
sleep 5

# Verify
echo "Step 6: Verifying replication..."
SOURCE_COUNT=$(docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -t -c "SELECT COUNT(*) FROM demo;")
SINK_COUNT=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "SELECT COUNT(*) FROM demo;")

SOURCE_COUNT=$(echo $SOURCE_COUNT | xargs)
SINK_COUNT=$(echo $SINK_COUNT | xargs)

echo "Source count: $SOURCE_COUNT"
echo "Sink count:   $SINK_COUNT"
echo ""

if [ "$SOURCE_COUNT" -eq "$SINK_COUNT" ] && [ "$SOURCE_COUNT" -eq "5" ]; then
    echo "✅ SUCCESS: Replication working!"
    echo ""
    echo "Data in sink:"
    docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -c "SELECT * FROM demo ORDER BY id;"
else
    echo "❌ FAILURE: Replication not working!"
    echo ""
    echo "Check logs: tail -f /tmp/replicator.log"
    echo ""
    echo "Common issues:"
    echo "  - Replication slot not created"
    echo "  - Publication not configured"
    echo "  - Connection string incorrect in config.yaml"
fi

echo ""
echo "Replicator is still running (PID: $REPLICATOR_PID)"
echo "To stop: kill $REPLICATOR_PID"
echo "To run stress test: ./scripts/stress_test.sh 1000 100"
