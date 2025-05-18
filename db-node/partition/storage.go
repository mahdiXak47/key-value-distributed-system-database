package partition

import (
	"sync"

	"github.com/mahdiXak47/key-value-distributed-system-database/db-node/lsm"
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
	if s.memTable.Size() >= 1000 { // Configurable threshold
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
	// Start from level 0
	for i := 0; i < len(s.levels); i++ {
		level := s.levels[i]

		// If level is full, merge with next level
		if len(level.immutableLayers) >= 5 { // Configurable threshold
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
func (s *Storage) GetMetrics() (int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.memTable.Size(), len(s.levels)
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
