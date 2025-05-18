package distribution

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"sync"
)

// Node represents a server node in the cluster
type Node struct {
	Address string
	Hash    uint32
}

// HashRing implements consistent hashing
type HashRing struct {
	mu            sync.RWMutex
	nodes         []Node
	virtualNodes  int // Number of virtual nodes per physical node
	numPartitions int
}

// NewHashRing creates a new hash ring with the specified number of virtual nodes and partitions
func NewHashRing(virtualNodes, numPartitions int) *HashRing {
	return &HashRing{
		virtualNodes:  virtualNodes,
		numPartitions: numPartitions,
	}
}

// AddNode adds a physical node to the hash ring with virtual nodes
func (hr *HashRing) AddNode(address string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	// Create virtual nodes for this physical node
	for i := 0; i < hr.virtualNodes; i++ {
		// Create a unique string for this virtual node
		virtualNodeStr := address + "#" + string(rune(i))
		hash := hashString(virtualNodeStr)

		hr.nodes = append(hr.nodes, Node{
			Address: address,
			Hash:    hash,
		})
	}

	// Sort nodes by hash value
	sort.Slice(hr.nodes, func(i, j int) bool {
		return hr.nodes[i].Hash < hr.nodes[j].Hash
	})
}

// RemoveNode removes a physical node and all its virtual nodes from the hash ring
func (hr *HashRing) RemoveNode(address string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	// Remove all virtual nodes for this physical node
	newNodes := make([]Node, 0, len(hr.nodes))
	for _, node := range hr.nodes {
		if node.Address != address {
			newNodes = append(newNodes, node)
		}
	}
	hr.nodes = newNodes
}

// GetNode returns the node responsible for a given key
func (hr *HashRing) GetNode(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.nodes) == 0 {
		return ""
	}

	// Hash the key
	keyHash := hashString(key)

	// Find the first node with hash greater than the key hash
	idx := sort.Search(len(hr.nodes), func(i int) bool {
		return hr.nodes[i].Hash > keyHash
	})

	// If we didn't find a node with greater hash, wrap around to the first node
	if idx == len(hr.nodes) {
		idx = 0
	}

	return hr.nodes[idx].Address
}

// GetPartition returns the partition number for a given key
func (hr *HashRing) GetPartition(key string) int {
	keyHash := hashString(key)
	return int(keyHash % uint32(hr.numPartitions))
}

// hashString generates a 32-bit hash from a string using SHA-256
func hashString(s string) uint32 {
	hash := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint32(hash[:4])
}

// GetNodes returns all nodes in the hash ring
func (hr *HashRing) GetNodes() []Node {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	nodes := make([]Node, len(hr.nodes))
	copy(nodes, hr.nodes)
	return nodes
}

// GetNodeCount returns the number of physical nodes
func (hr *HashRing) GetNodeCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	// Count unique addresses
	addresses := make(map[string]bool)
	for _, node := range hr.nodes {
		addresses[node.Address] = true
	}
	return len(addresses)
}
