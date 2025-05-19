package main

import (
	"log"

	lsm "github.com/mahdiXak47/key-value-distributed-system-database/db-node/lsm"
)

const (
	maxMemTableSize = 1000 // Maximum number of entries in MemTable
	maxLevelSize    = 5    // Maximum number of immutable layers per level
)

type Level struct {
	immutableLayers []*lsm.MemTable
}

type Storage struct {
	memTable *lsm.MemTable
	levels   []*Level
	wal      *lsm.WAL
}

func NewStorage() *Storage {
	return &Storage{
		memTable: lsm.NewMemTable(),
		levels:   make([]*Level, 0),
		wal:      lsm.NewWAL(),
	}
}

func (s *Storage) checkAndRotateMemTable() {
	if s.memTable.Size() >= maxMemTableSize {
		// Add checkpoint to WAL before rotating
		s.wal.AddCheckpoint()

		// Freeze current MemTable
		frozenTable := s.memTable

		// Create new MemTable for ongoing writes
		s.memTable = lsm.NewMemTable()

		// Add frozen table to the first level
		if len(s.levels) == 0 {
			s.levels = append(s.levels, &Level{
				immutableLayers: make([]*lsm.MemTable, 0),
			})
		}

		// Add to first level
		s.levels[0].immutableLayers = append(s.levels[0].immutableLayers, frozenTable)

		// Check if we need to merge levels
		s.checkAndMergeLevels()

		log.Println("Rotated MemTable due to size threshold")
	}
}

func (s *Storage) checkAndMergeLevels() {
	// Start from level 0
	for i := 0; i < len(s.levels); i++ {
		level := s.levels[i]

		// If level is full, merge with next level
		if len(level.immutableLayers) >= maxLevelSize {
			// Create next level if it doesn't exist
			if i+1 >= len(s.levels) {
				s.levels = append(s.levels, &Level{
					immutableLayers: make([]*lsm.MemTable, 0),
				})
			}

			// Merge all layers from current level into one
			mergedTable := lsm.NewMemTable()

			// Collect all key-value pairs from all layers
			// Later layers (newer data) will overwrite earlier ones
			for _, layer := range level.immutableLayers {
				// Get all key-value pairs from the layer
				layerData := layer.GetAll()

				// Add each key-value pair to the merged table
				for key, value := range layerData {
					mergedTable.Set(key, value)
				}
			}

			// Add merged table to next level
			s.levels[i+1].immutableLayers = append(s.levels[i+1].immutableLayers, mergedTable)

			// Clear current level
			level.immutableLayers = make([]*lsm.MemTable, 0)

			log.Printf("Merged level %d into level %d", i, i+1)
		}
	}
}

func (s *Storage) Set(key, value string) {
	// Write to WAL first
	s.wal.AddEntry("SET", key, value)

	// Then update storage
	s.checkAndRotateMemTable()
	s.memTable.Set(key, value)
}

func (s *Storage) Get(key string) (string, bool) {
	// First check in active MemTable
	value, exists := s.memTable.Get(key)
	if exists {
		return value, true
	}

	// Then check in each level's immutable layers
	for _, level := range s.levels {
		// Check layers in reverse order (newest first)
		for i := len(level.immutableLayers) - 1; i >= 0; i-- {
			value, exists = level.immutableLayers[i].Get(key)
			if exists {
				return value, true
			}
		}
	}

	return "", false
}

func (s *Storage) Delete(key string) {
	// Write to WAL first
	s.wal.AddEntry("DELETE", key, "")

	// Then update storage
	s.checkAndRotateMemTable()
	s.memTable.Delete(key)
}

// RecoverFromWAL replays the WAL entries to recover the storage state
func (s *Storage) RecoverFromWAL() {
	entries := s.wal.GetAllEntries()
	for _, entry := range entries {
		switch entry.Operation {
		case "SET":
			s.memTable.Set(entry.Key, entry.Value)
		case "DELETE":
			s.memTable.Delete(entry.Key)
		case "CHECKPOINT":
			// When we hit a checkpoint, we know all previous entries
			// are already in the immutable layers
			continue
		}
	}
}

// CreateSnapshot creates a new snapshot for replication
func (s *Storage) CreateSnapshot() *lsm.Snapshot {
	// Collect all immutable layers from all levels
	var immutableLayers []*lsm.MemTable
	for _, level := range s.levels {
		immutableLayers = append(immutableLayers, level.immutableLayers...)
	}

	// Get WAL entries since last checkpoint (active MemTable changes)
	walEntries := s.wal.GetEntriesSinceCheckpoint()

	return lsm.NewSnapshot(immutableLayers, walEntries)
}

// ApplySnapshot applies a complete snapshot to the storage
func (s *Storage) ApplySnapshot(snapshot *lsm.Snapshot) error {
	// Clear existing levels
	s.levels = make([]*Level, 0)

	// Add all immutable layers to the first level
	if len(snapshot.ImmutableLayers) > 0 {
		s.levels = append(s.levels, &Level{
			immutableLayers: snapshot.ImmutableLayers,
		})
	}

	// Apply WAL entries to the current MemTable
	for _, entry := range snapshot.WALEntries {
		switch entry.Operation {
		case "SET":
			s.memTable.Set(entry.Key, entry.Value)
		case "DELETE":
			s.memTable.Delete(entry.Key)
		}
	}

	return nil
}

// GarbageCollectLayers removes old immutable layers that are no longer needed
func (s *Storage) GarbageCollectLayers() {
	// Keep only the most recent layers from each level
	for i, level := range s.levels {
		if len(level.immutableLayers) > maxLevelSize {
			// Calculate how many layers to keep
			layersToKeep := maxLevelSize / 2 // Keep half of the maximum size

			// Remove older layers
			s.levels[i].immutableLayers = level.immutableLayers[len(level.immutableLayers)-layersToKeep:]
			log.Printf("Garbage collected %d old layers from level %d",
				len(level.immutableLayers)-layersToKeep, i)
		}
	}
}

// MergeOldLayers combines old immutable layers to free up memory
func (s *Storage) MergeOldLayers() {
	for i, level := range s.levels {
		if len(level.immutableLayers) >= maxLevelSize {
			// Create a new MemTable for merged data
			mergedTable := lsm.NewMemTable()

			// Merge older layers (first half)
			layersToMerge := level.immutableLayers[:len(level.immutableLayers)/2]
			for _, layer := range layersToMerge {
				data := layer.GetAll()
				for key, value := range data {
					mergedTable.Set(key, value)
				}
			}

			// Replace old layers with merged layer
			s.levels[i].immutableLayers = append(
				[]*lsm.MemTable{mergedTable},
				level.immutableLayers[len(level.immutableLayers)/2:]...,
			)

			log.Printf("Merged %d old layers in level %d", len(layersToMerge), i)
		}
	}
}

// CleanupSnapshot removes a snapshot after it's no longer needed
func (s *Storage) CleanupSnapshot(snapshot *lsm.Snapshot) {
	// Clear the snapshot's data
	snapshot.ImmutableLayers = nil
	snapshot.WALEntries = nil
}

// GetSnapshotForReplica creates a snapshot for a new replica
func (s *Storage) GetSnapshotForReplica() *lsm.Snapshot {
	// Create snapshot of current state
	snapshot := s.CreateSnapshot()

	// Add a checkpoint to mark the start of new WAL entries
	s.wal.AddCheckpoint()

	return snapshot
}

// GetWALEntriesSince returns WAL entries from a specific checkpoint
func (s *Storage) GetWALEntriesSince(checkpointIndex int) []lsm.LogEntry {
	allEntries := s.wal.GetAllEntries()
	if checkpointIndex < 0 || checkpointIndex >= len(allEntries) {
		return allEntries
	}
	return allEntries[checkpointIndex:]
}

// ApplyWALEntries applies a range of WAL entries to catch up
func (s *Storage) ApplyWALEntries(entries []lsm.LogEntry) error {
	for _, entry := range entries {
		switch entry.Operation {
		case "SET":
			s.memTable.Set(entry.Key, entry.Value)
		case "DELETE":
			s.memTable.Delete(entry.Key)
		case "CHECKPOINT":
			// Rotate current MemTable to immutable layer
			s.checkAndRotateMemTable()
		}
	}
	return nil
}

// GetLastCheckpointIndex returns the index of the last checkpoint in WAL
func (s *Storage) GetLastCheckpointIndex() int {
	entries := s.wal.GetAllEntries()
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Operation == "CHECKPOINT" {
			return i
		}
	}
	return -1
}

// GetMetrics returns current storage metrics
func (s *Storage) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"memTableSize": s.memTable.Size(),
		"levels":       len(s.levels),
	}
}
