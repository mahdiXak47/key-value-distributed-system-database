package lsm

import (
	"sync"
	"time"
)

type LogEntry struct {
	operation string
	key       string
	value     string
	timestamp time.Time
}

type WAL struct {
	entries []LogEntry
	mu      sync.Mutex
}

func NewWAL() *WAL {
	return &WAL{
		entries: []LogEntry{},
	}
}

func (wal *WAL) AddEntry(operation, key, value string) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	wal.entries = append(wal.entries, LogEntry{
		operation: operation,
		key:       key,
		value:     value,
		timestamp: time.Now(),
	})
}
