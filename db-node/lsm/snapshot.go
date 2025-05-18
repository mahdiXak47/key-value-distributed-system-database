package lsm

import (
	"encoding/json"
	"time"
)

type Snapshot struct {
	ImmutableLayers []*MemTable `json:"immutableLayers"` // All frozen MemTables
	WALEntries      []LogEntry  `json:"walEntries"`      // Pending WAL entries from active MemTable
	Timestamp       time.Time   `json:"timestamp"`
}

func NewSnapshot(immutableLayers []*MemTable, walEntries []LogEntry) *Snapshot {
	return &Snapshot{
		ImmutableLayers: immutableLayers,
		WALEntries:      walEntries,
		Timestamp:       time.Now(),
	}
}

// GetImmutableLayers returns all immutable layers for bulk transfer
func (snapshot *Snapshot) GetImmutableLayers() []*MemTable {
	return snapshot.ImmutableLayers
}

// GetWALEntries returns all pending WAL entries
func (snapshot *Snapshot) GetWALEntries() []LogEntry {
	return snapshot.WALEntries
}

func (snapshot *Snapshot) Get(key string) (string, bool) {
	// Check immutable layers first (newest to oldest)
	for i := len(snapshot.ImmutableLayers) - 1; i >= 0; i-- {
		if value, exists := snapshot.ImmutableLayers[i].Get(key); exists {
			return value, true
		}
	}

	// Check WAL entries (newest to oldest)
	for i := len(snapshot.WALEntries) - 1; i >= 0; i-- {
		entry := snapshot.WALEntries[i]
		if entry.Key == key {
			if entry.Operation == "DELETE" {
				return "", false
			}
			return entry.Value, true
		}
	}

	return "", false
}

// ToJSON converts the snapshot to JSON format for transmission
func (snapshot *Snapshot) ToJSON() ([]byte, error) {
	return json.Marshal(snapshot)
}

// FromJSON creates a snapshot from JSON data
func FromJSON(data []byte) (*Snapshot, error) {
	var snapshot Snapshot
	err := json.Unmarshal(data, &snapshot)
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}
