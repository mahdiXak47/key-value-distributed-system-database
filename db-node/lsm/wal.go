package lsm

import (
	"sync"
	"time"
)

// LogEntry represents a single entry in the WAL
type LogEntry struct {
	Operation string    `json:"operation"` // "SET", "DELETE", or "CHECKPOINT"
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// WAL represents the Write-Ahead Log
type WAL struct {
	entries []LogEntry
	mu      sync.RWMutex
}

// NewWAL creates a new WAL instance
func NewWAL() *WAL {
	return &WAL{
		entries: make([]LogEntry, 0),
	}
}

// AddEntry adds a new pre-constructed entry to the WAL
func (w *WAL) AddEntry(entry LogEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// It's assumed the caller has already set the Timestamp on the entry
	w.entries = append(w.entries, entry)
}

// AddCheckpoint adds a checkpoint entry to the WAL
func (w *WAL) AddCheckpoint() {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := LogEntry{
		Operation: "CHECKPOINT",
		Timestamp: time.Now(),
	}
	w.entries = append(w.entries, entry)
}

// GetEntriesSinceCheckpoint returns all entries since the last checkpoint
func (w *WAL) GetEntriesSinceCheckpoint() []LogEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Find the last checkpoint
	lastCheckpoint := -1
	for i := len(w.entries) - 1; i >= 0; i-- {
		if w.entries[i].Operation == "CHECKPOINT" {
			lastCheckpoint = i
			break
		}
	}

	// Return all entries after the last checkpoint
	if lastCheckpoint == -1 {
		return w.entries
	}
	return w.entries[lastCheckpoint+1:]
}

// GetAllEntries returns all entries in the WAL
func (w *WAL) GetAllEntries() []LogEntry {
	w.mu.RLock()
	defer w.mu.RUnlock()

	// Create a copy of entries
	entries := make([]LogEntry, len(w.entries))
	copy(entries, w.entries)
	return entries
}
