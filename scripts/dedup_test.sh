#!/bin/bash
set -e

echo "==================================="
echo "Deduplication Test"
echo "==================================="

# Get initial count
INITIAL_COUNT=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "SELECT COUNT(*) FROM stress_test;" | xargs)
echo "Initial sink count: $INITIAL_COUNT"
echo ""

# Insert some new rows in source
echo "Inserting 100 new rows (IDs 200000-200099)..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO stress_test (id, data)
SELECT i, 'Dedup test ' || i
FROM generate_series(200000, 200099) AS i;
" > /dev/null

echo "Waiting 5 seconds for replication..."
sleep 5

AFTER_INSERT=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "SELECT COUNT(*) FROM stress_test;" | xargs)
echo "After insert: $AFTER_INSERT (expected: $(($INITIAL_COUNT + 100)))"
echo ""

# Now restart replicator to trigger replay
echo "Killing replicator to simulate crash..."
pkill -f bin/replicator
sleep 2

echo "Restarting replicator (may replay recent events)..."
./bin/replicator -config config.yaml > /tmp/replicator_dedup.log 2>&1 &
REPLICATOR_PID=$!
echo "Replicator restarted (PID: $REPLICATOR_PID)"
sleep 3

# Insert the SAME rows again (should be deduplicated)
echo ""
echo "Re-inserting the same 100 rows (IDs 200000-200099) - should be deduplicated..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO stress_test (id, data)
SELECT i, 'Dedup test ' || i
FROM generate_series(200000, 200099) AS i
ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
" > /dev/null

echo "Waiting 5 seconds for replication..."
sleep 5

AFTER_REINSERT=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "SELECT COUNT(*) FROM stress_test;" | xargs)
echo "After re-insert: $AFTER_REINSERT"
echo ""

# Verify deduplication
if [ "$AFTER_REINSERT" -eq "$AFTER_INSERT" ]; then
    echo "✅ SUCCESS: Deduplication working!"
    echo "   Count stayed at $AFTER_REINSERT (no duplicates created)"
else
    echo "❌ FAILURE: Duplicates detected!"
    echo "   Expected: $AFTER_INSERT"
    echo "   Got: $AFTER_REINSERT"
    echo "   Difference: $(($AFTER_REINSERT - $AFTER_INSERT))"
fi

echo ""
echo "Checking for actual duplicates in sink..."
DUPES=$(docker exec -i replicator-pg_sink-1 psql -U postgres -d sink_db -t -c "
SELECT COUNT(*) - COUNT(DISTINCT id) as duplicate_count FROM stress_test;
" | xargs)

if [ "$DUPES" -eq "0" ]; then
    echo "✅ No duplicate IDs found in sink"
else
    echo "❌ Found $DUPES duplicate IDs!"
fi
