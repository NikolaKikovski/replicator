package types

import (
	"fmt"
	"time"
)

type LSN uint64

func (l LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l))
}

type EventType string

const (
	EventInsert EventType = "INSERT"
	EventUpdate EventType = "UPDATE"
	EventDelete EventType = "DELETE"
	EventCommit EventType = "COMMIT" // Used for checkpointing
)

type Event struct {
	Type      EventType
	Schema    string
	Table     string
	Columns   map[string]interface{} // New values
	Identity  map[string]interface{} // Key values (for Update/Delete)
	LSN       LSN
	Timestamp time.Time
}

type Batch struct {
	Events []*Event
	MaxLSN LSN
}
