package pipeline

import (
	"testing"

	"github.com/nikolay-makurin/replicator/pkg/types"
)

func TestCheckpointManager(t *testing.T) {
	cm := NewCheckpointManager(types.LSN(100))

	// Track 101, 102, 103
	cm.Track(types.LSN(101))
	cm.Track(types.LSN(102))
	cm.Track(types.LSN(103))

	// Mark 103 done (out of order)
	cm.MarkDone(types.LSN(103))
	if safe := cm.GetSafeLSN(); safe != 100 {
		t.Errorf("Expected safe LSN 100, got %d", safe)
	}

	// Mark 101 done
	cm.MarkDone(types.LSN(101))
	if safe := cm.GetSafeLSN(); safe != 101 {
		t.Errorf("Expected safe LSN 101, got %d", safe)
	}

	// Mark 102 done
	cm.MarkDone(types.LSN(102))
	if safe := cm.GetSafeLSN(); safe != 103 {
		t.Errorf("Expected safe LSN 103, got %d", safe)
	}
}
