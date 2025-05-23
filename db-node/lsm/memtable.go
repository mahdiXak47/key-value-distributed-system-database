package lsm

import (
	"sync"
)

type MemTable struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string]string),
	}
}

func (mt *MemTable) Get(key string) (string, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	value, exists := mt.data[key]
	return value, exists
}

func (mt *MemTable) Set(key, value string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.data[key] = value
}

func (mt *MemTable) Delete(key string) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	delete(mt.data, key)
}

func (mt *MemTable) Size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return len(mt.data)
}

// GetAll returns a copy of all key-value pairs in the MemTable
func (mt *MemTable) GetAll() map[string]string {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	// Create a copy of the data
	copy := make(map[string]string, len(mt.data))
	for k, v := range mt.data {
		copy[k] = v
	}
	return copy
}
