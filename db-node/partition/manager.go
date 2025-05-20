package partition

// GetPartitionIDs returns a list of all partition IDs this node is responsible for
func (m *Manager) GetPartitionIDs() []int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]int, 0, len(m.partitions))
	for id := range m.partitions {
		ids = append(ids, id)
	}
	return ids
}
