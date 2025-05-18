package main

import (
	"log"

	lsm "github.com/mahdiXak47/key-value-distributed-system-database/db-node/lsm"
)

const (
	maxMemTableSize = 1000 // Maximum number of entries in MemTable
)

type Storage struct {
	memTable        *lsm.MemTable
	immutableLayers []*lsm.MemTable
}

func NewStorage() *Storage {
	return &Storage{
		memTable:        lsm.NewMemTable(),
		immutableLayers: make([]*lsm.MemTable, 0),
	}
}

func (s *Storage) checkAndRotateMemTable() {
	if s.memTable.Size() >= maxMemTableSize {
		// Freeze current MemTable
		frozenTable := s.memTable
		s.immutableLayers = append(s.immutableLayers, frozenTable)

		// Create new MemTable for ongoing writes
		s.memTable = lsm.NewMemTable()
		log.Println("Rotated MemTable due to size threshold")
	}
}

func (s *Storage) Set(key, value string) {
	s.checkAndRotateMemTable()
	s.memTable.Set(key, value)
}

func (s *Storage) Get(key string) (string, bool) {
	// First check in active MemTable
	value, exists := s.memTable.Get(key)
	if exists {
		return value, true
	}

	// Then check in immutable layers (from newest to oldest)
	for i := len(s.immutableLayers) - 1; i >= 0; i-- {
		value, exists = s.immutableLayers[i].Get(key)
		if exists {
			return value, true
		}
	}

	return "", false
}

func (s *Storage) Delete(key string) {
	s.checkAndRotateMemTable()
	s.memTable.Delete(key)
}
