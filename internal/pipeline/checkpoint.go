package pipeline

import (
	"container/heap"
	"sync"

	"github.com/nikolay-makurin/replicator/pkg/types"
)

// LSNHeap implements heap.Interface for types.LSN
type LSNHeap []types.LSN

func (h LSNHeap) Len() int           { return len(h) }
func (h LSNHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h LSNHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *LSNHeap) Push(x interface{}) {
	*h = append(*h, x.(types.LSN))
}

func (h *LSNHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type CheckpointManager struct {
	mu           sync.Mutex
	inflight     LSNHeap       // Min-heap of all tracked LSNs
	done         map[types.LSN]bool
	lastSafeLSN  types.LSN
}

func NewCheckpointManager(startLSN types.LSN) *CheckpointManager {
	cm := &CheckpointManager{
		inflight:    make(LSNHeap, 0),
		done:        make(map[types.LSN]bool),
		lastSafeLSN: startLSN,
	}
	heap.Init(&cm.inflight)
	return cm
}

func (cm *CheckpointManager) Track(lsn types.LSN) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	heap.Push(&cm.inflight, lsn)
}

func (cm *CheckpointManager) MarkDone(lsn types.LSN) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.done[lsn] = true

	// Advance safe LSN
	for cm.inflight.Len() > 0 {
		min := cm.inflight[0]
		if cm.done[min] {
			heap.Pop(&cm.inflight)
			delete(cm.done, min)
			cm.lastSafeLSN = min
		} else {
			break
		}
	}
}

func (cm *CheckpointManager) GetSafeLSN() types.LSN {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.lastSafeLSN
}
