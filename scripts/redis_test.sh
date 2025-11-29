#!/bin/bash
set -e

echo "==================================="
echo "Redis Sink Verification"
echo "==================================="

# Ensure Redis is running
if ! docker ps | grep -q replicator-redis-1; then
    echo "❌ Redis container not running!"
    exit 1
fi

echo "✅ Redis container is running"

# Insert test data
echo "Inserting test row into source..."
docker exec -i replicator-pg_source-1 psql -U postgres -d source_db -c "
INSERT INTO stress_test (id, data) VALUES (999999, 'Redis Test Data')
ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data;
" > /dev/null

echo "Waiting 5 seconds for replication..."
sleep 5

# Check Redis
echo "Checking Redis for key 'stress_test:999999'..."
REDIS_VAL=$(docker exec -i replicator-redis-1 redis-cli GET "stress_test:999999")

if [ -z "$REDIS_VAL" ]; then
    echo "❌ FAILURE: Key not found in Redis!"
    exit 1
fi

echo "Found value: $REDIS_VAL"

# Verify content
if [[ "$REDIS_VAL" == *"Redis Test Data"* ]]; then
    echo "✅ SUCCESS: Data replicated to Redis correctly!"
else
    echo "❌ FAILURE: Data mismatch!"
    echo "Expected to contain: 'Redis Test Data'"
    echo "Got: $REDIS_VAL"
    exit 1
fi
