package storage

import (
	"log"

	"github.com/mahdiXak47/key-value-distributed-system-database/db-node/lsm"
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

// GetMetrics returns current storage metrics
func (s *Storage) GetMetrics() (int, int) {
	return s.memTable.Size(), len(s.levels)
}
