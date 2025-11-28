#!/bin/bash
set -e

echo "==================================="
echo "Replicator Stress Test"
echo "==================================="

# Configuration
NUM_ROWS=${1:-100000}
BATCH_SIZE=${2:-1000}

echo "Stress test parameters:"
echo "  Rows to insert: $NUM_ROWS"
echo "  Batch size: $BATCH_SIZE"
echo ""

# Check if replicator is running
if ! pgrep -f "bin/replicator" > /dev/null; then
    echo "❌ ERROR: Replicator is not running!"
    echo ""
    echo "Please run the end-to-end test first to start the replicator:"
    echo "  make e2e-test"
    echo ""
    echo "OR start the replicator manually:"
    echo "  ./bin/replicator -config config.yaml &"
    echo ""
    exit 1
fi
echo "✅ Replicator is running"
echo ""

# Wait for databases
echo "Waiting for databases to be ready..."
until docker exec replicator-pg_source-1 pg_isready -U postgres 2>/dev/null; do
  echo "  Waiting for source database..."
  sleep 1
done
until docker exec replicator-pg_sink-1 pg_isready -U postgres 2>/dev/null; do
  echo "  Waiting for sink database..."
  sleep 1
done
echo "Databases ready!"
echo ""

# Setup source
echo "Setting up source database..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db <<EOF
DROP TABLE IF EXISTS stress_test CASCADE;
CREATE TABLE stress_test (
    id SERIAL PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create publication if not exists
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'my_pub') THEN
        CREATE PUBLICATION my_pub FOR ALL TABLES;
    END IF;
END \$\$;

-- Set replica identity
ALTER TABLE stress_test REPLICA IDENTITY FULL;

-- Create replication slot if not exists
SELECT * FROM pg_create_logical_replication_slot('replicator_slot', 'pgoutput', false, false)
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replicator_slot');
EOF

# Setup sink
echo "Setting up sink database..."
docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db <<EOF
DROP TABLE IF EXISTS stress_test;
CREATE TABLE stress_test (
    id INT PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMPTZ
);
EOF

echo ""
echo "==================================="
echo "Starting stress test..."
echo "==================================="

# Insert data in batches
echo "Inserting $NUM_ROWS rows..."
START_TIME=$(date +%s)

for i in $(seq 1 $BATCH_SIZE $NUM_ROWS); do
    END_VAL=$(($i + $BATCH_SIZE - 1))
    if [ $END_VAL -gt $NUM_ROWS ]; then
        END_VAL=$NUM_ROWS
    fi
    
    docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
    INSERT INTO stress_test (data)
    SELECT 'Test data ' || generate_series($i, $END_VAL);
    " > /dev/null
    
    if [ $(($i % 10000)) -eq 1 ] || [ $i -eq 1 ]; then
        echo "  Inserted $i rows..."
    fi
done

END_TIME=$(date +%s)
DURATION=$(($END_TIME - $START_TIME))
echo "Insertion complete in ${DURATION}s"
echo ""

# Wait for replication
echo "Waiting for replication to complete..."
sleep 5

# Check replication lag
echo "Checking replication status..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
SELECT slot_name, 
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
FROM pg_replication_slots 
WHERE slot_name = 'replicator_slot';
"

# Verify counts
echo ""
echo "Verifying data..."
SOURCE_COUNT=$(docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -t -c "SELECT COUNT(*) FROM stress_test;")
SINK_COUNT=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "SELECT COUNT(*) FROM stress_test;")

SOURCE_COUNT=$(echo $SOURCE_COUNT | xargs)
SINK_COUNT=$(echo $SINK_COUNT | xargs)

echo "Source count: $SOURCE_COUNT"
echo "Sink count:   $SINK_COUNT"
echo ""

if [ "$SOURCE_COUNT" -eq "$SINK_COUNT" ]; then
    echo "✅ SUCCESS: Counts match!"
    if [ $DURATION -gt 0 ]; then
        THROUGHPUT=$(echo "scale=2; $SOURCE_COUNT / $DURATION" | bc)
        echo "   Throughput: ${THROUGHPUT} rows/sec"
    fi
else
    echo "❌ FAILURE: Count mismatch!"
    echo "   Missing: $(($SOURCE_COUNT - $SINK_COUNT)) rows"
    exit 1
fi
