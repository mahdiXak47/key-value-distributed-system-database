package partition

import (
	"sync"
)

// Role represents the role of a node for a partition
type Role string

const (
	RoleLeader  Role = "leader"
	RoleReplica Role = "replica"
)

// Partition represents a data partition
type Partition struct {
	ID       int
	Role     Role
	Leader   string
	Replicas []string
	storage  *Storage
	mu       sync.RWMutex
}

// Manager handles partition management for a DB node
type Manager struct {
	partitions map[int]*Partition
	mu         sync.RWMutex
}

// NewManager creates a new partition manager
func NewManager() *Manager {
	return &Manager{
		partitions: make(map[int]*Partition),
	}
}

// NewPartition creates a new partition
func NewPartition(id int, role Role, leader string, replicas []string) *Partition {
	return &Partition{
		ID:       id,
		Role:     role,
		Leader:   leader,
		Replicas: replicas,
		storage:  NewStorage(id),
	}
}

// AddPartition adds a new partition to the manager
func (m *Manager) AddPartition(id int, role Role, leader string, replicas []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.partitions[id] = &Partition{
		ID:       id,
		Role:     role,
		Leader:   leader,
		Replicas: replicas,
		storage:  NewStorage(id),
	}
}

// RemovePartition removes a partition from the manager
func (m *Manager) RemovePartition(id int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.partitions, id)
}

// GetPartition returns a partition by ID
func (m *Manager) GetPartition(id int) *Partition {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.partitions[id]
}

// HasPartition checks if a partition exists
func (m *Manager) HasPartition(id int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.partitions[id]
	return exists
}

// GetPartitions returns all partitions
func (m *Manager) GetPartitions() []*Partition {
	m.mu.RLock()
	defer m.mu.RUnlock()

	partitions := make([]*Partition, 0, len(m.partitions))
	for _, p := range m.partitions {
		partitions = append(partitions, p)
	}
	return partitions
}

// UpdateRole updates the role of a partition
func (m *Manager) UpdateRole(id int, role Role) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, exists := m.partitions[id]; exists {
		p.mu.Lock()
		p.Role = role
		p.mu.Unlock()
	}
}

// UpdateLeader updates the leader of a partition
func (m *Manager) UpdateLeader(id int, leader string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, exists := m.partitions[id]; exists {
		p.mu.Lock()
		p.Leader = leader
		p.mu.Unlock()
	}
}

// UpdateReplicas updates the replicas of a partition
func (m *Manager) UpdateReplicas(id int, replicas []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p, exists := m.partitions[id]; exists {
		p.mu.Lock()
		p.Replicas = replicas
		p.mu.Unlock()
	}
}

// IsLeader checks if this node is the leader for a partition
func (m *Manager) IsLeader(id int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if p, exists := m.partitions[id]; exists {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.Role == RoleLeader
	}
	return false
}

// IsReplica checks if this node is a replica for a partition
func (m *Manager) IsReplica(id int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if p, exists := m.partitions[id]; exists {
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.Role == RoleReplica
	}
	return false
}

func (p *Partition) GetStorage() *Storage {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.storage
}
