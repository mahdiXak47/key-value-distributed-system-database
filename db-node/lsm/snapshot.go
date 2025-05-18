package lsm

type Snapshot struct {
	immutableLayers []ImmutableLayer
	walEntries      []LogEntry
}

func NewSnapshot(immutableLayers []ImmutableLayer, walEntries []LogEntry) *Snapshot {
	return &Snapshot{
		immutableLayers: immutableLayers,
		walEntries:      walEntries,
	}
}

func (snapshot *Snapshot) Get(key string) (string, bool) {
	// Check immutable layers first
	for _, layer := range snapshot.immutableLayers {
		if value, exists := layer.Get(key); exists {
			return value, true
		}
	}
	// Check WAL entries
	for _, entry := range snapshot.walEntries {
		if entry.key == key {
			return entry.value, true
		}
	}
	return "", false
}
