#!/bin/bash
set -e

echo "==================================="
echo "Performance Benchmark"
echo "==================================="

SOURCE_DB="postgres://postgres:password@localhost:15432/source_db"

# Test different batch sizes
for BATCH_SIZE in 100 500 1000 5000 10000; do
    echo ""
    echo "Testing batch size: $BATCH_SIZE"
    echo "-----------------------------------"
    
    # Update config
    sed -i.bak "s/batch_size: [0-9]*/batch_size: $BATCH_SIZE/" config.yaml
    
    # Restart replicator (assumes it's managed externally)
    echo "Update config.yaml with batch_size: $BATCH_SIZE and restart replicator"
    
    # Run a mini stress test
    NUM_ROWS=10000
    ./scripts/stress_test.sh $NUM_ROWS $BATCH_SIZE || true
done

# Restore original config
mv config.yaml.bak config.yaml

echo ""
echo "Benchmark complete!"
