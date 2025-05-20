package partition

import (
	"sync"

	"github.com/mahdiXak47/key-value-distributed-system-database/db-node/lsm"
)

const (
	maxMemTableSize = 1000 // Maximum number of entries in MemTable
	maxLevelSize    = 5    // Maximum number of immutable layers per level
)

// Storage represents partition-specific storage
type Storage struct {
	partitionID int
	memTable    *lsm.MemTable
	levels      []*Level
	wal         *lsm.WAL
	mu          sync.RWMutex
}

// Level represents a level in the LSM tree
type Level struct {
	immutableLayers []*lsm.MemTable
}

// NewStorage creates a new partition-specific storage
func NewStorage(partitionID int) *Storage {
	return &Storage{
		partitionID: partitionID,
		memTable:    lsm.NewMemTable(),
		levels:      make([]*Level, 0),
		wal:         lsm.NewWAL(),
	}
}

// Set stores a key-value pair in the partition
func (s *Storage) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to WAL first
	s.wal.AddEntry("SET", key, value)

	// Then update storage
	s.checkAndRotateMemTable()
	s.memTable.Set(key, value)
}

// Get retrieves a value for a key from the partition
func (s *Storage) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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

// Delete removes a key from the partition
func (s *Storage) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to WAL first
	s.wal.AddEntry("DELETE", key, "")

	// Then update storage
	s.checkAndRotateMemTable()
	s.memTable.Delete(key)
}

// checkAndRotateMemTable checks if the MemTable needs to be rotated
func (s *Storage) checkAndRotateMemTable() {
	if s.memTable.Size() >= maxMemTableSize { // Use constant
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
	}
}

// checkAndMergeLevels checks if levels need to be merged
func (s *Storage) checkAndMergeLevels() {
	// No s.mu.Lock() here as this is an internal method called by checkAndRotateMemTable,
	// which is called by Set/Delete which already hold the lock.

	// Start from level 0
	for i := 0; i < len(s.levels); i++ {
		level := s.levels[i]

		// If level is full, merge with next level
		if len(level.immutableLayers) >= maxLevelSize { // Use constant
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
		}
	}
}

// GetMetrics returns storage metrics
func (s *Storage) GetMetrics() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]interface{}{
		"memTableSize": s.memTable.Size(),
		"levels":       len(s.levels),
	}
}

// CreateSnapshot creates a snapshot of the partition's data
func (s *Storage) CreateSnapshot() *lsm.Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Collect all immutable layers from all levels
	var immutableLayers []*lsm.MemTable
	for _, level := range s.levels {
		immutableLayers = append(immutableLayers, level.immutableLayers...)
	}

	// Get WAL entries since last checkpoint (active MemTable changes)
	walEntries := s.wal.GetEntriesSinceCheckpoint()

	return lsm.NewSnapshot(immutableLayers, walEntries)
}

// ApplySnapshot applies a snapshot to the partition
func (s *Storage) ApplySnapshot(snapshot *lsm.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

// RecoverFromWAL replays the WAL entries to recover the storage state
func (s *Storage) RecoverFromWAL() {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := s.wal.GetAllEntries()
	for _, entry := range entries {
		switch entry.Operation {
		case "SET":
			// directly set to memtable, rotation will be handled if needed by next regular SET
			s.memTable.Set(entry.Key, entry.Value)
		case "DELETE":
			s.memTable.Delete(entry.Key)
		case "CHECKPOINT":
			// Checkpoints are markers, actual data already processed or will be.
			// For recovery, we process all SET/DELETE ops.
			// If rotation logic were tied to checkpoints during replay, it would be more complex.
			// Current checkAndRotate is based on size, which is fine.
			continue
		}
	}
	// After replaying, it might be good to force a checkAndRotateMemTable
	// if the memTable became too large, but this is optional.
	// s.checkAndRotateMemTable()
}

// GetSnapshotForReplica creates a snapshot for a new replica and adds a WAL checkpoint.
func (s *Storage) GetSnapshotForReplica() *lsm.Snapshot {
	// CreateSnapshot needs RLock, AddCheckpoint needs Lock.
	// To avoid lock escalation issues or holding a lock for too long,
	// we can unlock RLock before acquiring Lock if CreateSnapshot is purely read-only on s fields.
	// However, CreateSnapshot already takes s.mu.RLock().
	// The WAL checkpointing should ideally be atomic with the snapshot perception.
	// Let's take a full lock for this composite operation.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create snapshot of current state
	// Collect all immutable layers from all levels
	var immutableLayers []*lsm.MemTable
	for _, level := range s.levels {
		immutableLayers = append(immutableLayers, level.immutableLayers...)
	}
	// Get WAL entries since last checkpoint (active MemTable changes)
	walEntries := s.wal.GetEntriesSinceCheckpoint()
	snapshot := lsm.NewSnapshot(immutableLayers, walEntries)

	// Add a checkpoint to mark the start of new WAL entries for the replica
	s.wal.AddCheckpoint()

	return snapshot
}

// GetWALEntriesSince returns WAL entries from a specific checkpoint index.
// Assumes lsm.WAL is concurrency-safe for read operations like GetAllEntries.
func (s *Storage) GetWALEntriesSince(checkpointIndex int) []lsm.LogEntry {
	// This method primarily interacts with WAL, assume WAL is thread-safe.
	// No explicit s.mu lock needed here if it only touches s.wal methods.
	allEntries := s.wal.GetAllEntries()
	if checkpointIndex < 0 || checkpointIndex >= len(allEntries) {
		// Return all entries if checkpointIndex is invalid, or consider error/empty slice
		return allEntries
	}
	return allEntries[checkpointIndex:]
}

// ApplyWALEntries applies a range of WAL entries to catch up.
func (s *Storage) ApplyWALEntries(entries []lsm.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range entries {
		switch entry.Operation {
		case "SET":
			s.memTable.Set(entry.Key, entry.Value)
		case "DELETE":
			s.memTable.Delete(entry.Key)
		case "CHECKPOINT":
			// If a checkpoint from the leader implies a rotation,
			// the replica should also rotate.
			s.checkAndRotateMemTable()
		}
	}
	// It's possible the memtable grew large after applying entries
	s.checkAndRotateMemTable() // ensure consistency with size limits
	return nil
}

// GetLastCheckpointIndex returns the index of the last checkpoint in WAL.
// Assumes lsm.WAL is concurrency-safe for read operations.
func (s *Storage) GetLastCheckpointIndex() int {
	// This method primarily interacts with WAL.
	entries := s.wal.GetAllEntries()
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].Operation == "CHECKPOINT" {
			return i
		}
	}
	return -1 // No checkpoint found
}

// GarbageCollectLayers removes old immutable layers that are no longer needed.
func (s *Storage) GarbageCollectLayers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Keep only the most recent layers from each level
	for i, level := range s.levels {
		if len(level.immutableLayers) > maxLevelSize { // Use constant
			// Calculate how many layers to keep
			layersToKeep := maxLevelSize / 2 // Keep half of the maximum size, ensure it's at least 1 if maxLevelSize is small
			if layersToKeep == 0 && maxLevelSize > 0 {
				layersToKeep = 1
			}

			if len(level.immutableLayers) > layersToKeep {
				s.levels[i].immutableLayers = level.immutableLayers[len(level.immutableLayers)-layersToKeep:]
			}
		}
	}
}

// MergeOldLayers combines old immutable layers to free up memory and reduce layers.
func (s *Storage) MergeOldLayers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, level := range s.levels {
		// This condition is similar to checkAndMergeLevels, but focuses on merging older parts
		// Let's refine this: if a level has too many layers, merge the oldest ones.
		if len(level.immutableLayers) >= maxLevelSize { // Use constant
			// Create a new MemTable for merged data
			mergedTable := lsm.NewMemTable()

			// Merge older layers (e.g., first half or those exceeding a threshold)
			// Example: merge all but the newest maxLevelSize/2 layers
			numToKeepIdeally := maxLevelSize / 2
			if numToKeepIdeally == 0 && maxLevelSize > 0 {
				numToKeepIdeally = 1
			}
			if len(level.immutableLayers) > numToKeepIdeally {
				layersToMergeCount := len(level.immutableLayers) - numToKeepIdeally
				layersToMerge := level.immutableLayers[:layersToMergeCount]
				remainingLayers := level.immutableLayers[layersToMergeCount:]

				for _, layer := range layersToMerge {
					data := layer.GetAll()
					for key, value := range data {
						mergedTable.Set(key, value) // Newer entries in later layersToMerge will overwrite older
					}
				}
				// Replace old layers with the merged layer
				s.levels[i].immutableLayers = append([]*lsm.MemTable{mergedTable}, remainingLayers...)
			}
		}
	}
}

// CleanupSnapshot clears data from a snapshot object to free memory.
// The snapshot object itself is passed by value or pointer, this helps clear its internal slices.
func (s *Storage) CleanupSnapshot(snapshot *lsm.Snapshot) {
	// This method operates on the snapshot object, not directly on s.Storage state.
	// No s.mu lock needed here.
	if snapshot != nil {
		snapshot.ImmutableLayers = nil
		snapshot.WALEntries = nil
	}
}
