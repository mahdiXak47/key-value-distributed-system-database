package lsm

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	dataDir string
	walFile *os.File
	encoder *json.Encoder
}

// NewWAL creates a new WAL instance
func NewWAL() *WAL {
	// Create data directory structure
	dataDir := "data"
	walDir := filepath.Join(dataDir, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		panic(fmt.Sprintf("Failed to create WAL directory: %v", err))
	}

	// Open WAL file
	walPath := filepath.Join(walDir, "wal.json")
	walFile, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open WAL file: %v", err))
	}

	// Read existing entries
	var entries []LogEntry
	decoder := json.NewDecoder(walFile)
	for {
		var entry LogEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			// If there's an error reading, start fresh
			entries = make([]LogEntry, 0)
			walFile.Truncate(0)
			walFile.Seek(0, 0)
			break
		}
		entries = append(entries, entry)
	}

	return &WAL{
		entries: entries,
		dataDir: dataDir,
		walFile: walFile,
		encoder: json.NewEncoder(walFile),
	}
}

// AddEntry adds a new pre-constructed entry to the WAL
func (w *WAL) AddEntry(entry LogEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Write to WAL file first
	if err := w.encoder.Encode(entry); err != nil {
		panic(fmt.Sprintf("Failed to write to WAL: %v", err))
	}
	w.walFile.Sync() // Force write to disk

	// Then update in-memory state
	w.entries = append(w.entries, entry)
}

// AddCheckpoint adds a checkpoint entry to the WAL
func (w *WAL) AddCheckpoint() {
	entry := LogEntry{
		Operation: "CHECKPOINT",
		Timestamp: time.Now(),
	}
	w.AddEntry(entry)
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

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.walFile.Close()
}

// Truncate removes all entries up to the last checkpoint
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Find the last checkpoint
	lastCheckpoint := -1
	for i := len(w.entries) - 1; i >= 0; i-- {
		if w.entries[i].Operation == "CHECKPOINT" {
			lastCheckpoint = i
			break
		}
	}

	if lastCheckpoint == -1 {
		return nil // Nothing to truncate
	}

	// Keep only entries after the last checkpoint
	w.entries = w.entries[lastCheckpoint:]

	// Rewrite the WAL file
	if err := w.walFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate WAL file: %v", err)
	}
	if _, err := w.walFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek WAL file: %v", err)
	}

	// Write all remaining entries
	encoder := json.NewEncoder(w.walFile)
	for _, entry := range w.entries {
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("failed to write entry during truncate: %v", err)
		}
	}
	return w.walFile.Sync()
}
