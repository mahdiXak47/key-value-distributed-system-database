package cluster

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	ID      int
	Name    string
	Address string
	Active  bool // true means up , false means down
}

type Partition struct {
	ID           int
	LeaderID     int   // node if of the leader node
	Replicas     []int // node ids that have this partition including leader
	LastLeaderID int   // if leader changes, last leader is previous leader
}

type Cluster struct {
	mu         sync.RWMutex
	nodes      map[int]*Node
	partitions map[int]*Partition
	nextNodeID int
	nextPartID int
}

func NewCluster() *Cluster {
	return &Cluster{
		nodes:      make(map[int]*Node),
		partitions: make(map[int]*Partition),
	}
}

func (c *Cluster) StartHealthChecks(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		c.mu.Lock()
		for _, node := range c.nodes {
			// Simulate random health changes
			if node.Active && rand.Float32() < 0.1 {
				node.Active = false
				log.Printf("Node %d (%s) went down", node.ID, node.Name)
			} else if !node.Active && rand.Float32() < 0.1 {
				node.Active = true
				log.Printf("Node %d (%s) came back up", node.ID, node.Name)
			}
		}
		// Check each partition for leader failure
		for _, part := range c.partitions {
			if leader, ok := c.nodes[part.LeaderID]; ok && !leader.Active {
				// leader is down: pick a new active replica
				oldLeader := part.LeaderID
				found := false
				for _, nid := range part.Replicas {
					if node, ok := c.nodes[nid]; ok && node.Active {
						part.LeaderID = nid
						part.LastLeaderID = oldLeader
						found = true
						log.Printf("Partition %d new leader is Node %d", part.ID, nid)
						break
					}
				}
				if !found {
					log.Printf("Partition %d has no active replicas", part.ID)
				}
			}
			// If old leader has come back, simulate instructions
			if part.LastLeaderID != 0 {
				if oldNode, ok := c.nodes[part.LastLeaderID]; ok && oldNode.Active {
					newLeader := part.LeaderID
					log.Printf("New leader %d will instruct old leader %d for partition %d", newLeader, oldNode.ID, part.ID)
					// Reset LastLeaderID after handling
					part.LastLeaderID = 0
				}
			}
		}
		c.mu.Unlock()
	}
}

func (c *Cluster) AddNode(name, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate input
	if name == "" || address == "" {
		return fmt.Errorf("name and address are required")
	}

	// Check if node with same address already exists
	for _, node := range c.nodes {
		if node.Address == address {
			return fmt.Errorf("node with address %s already exists", address)
		}
	}

	c.nextNodeID++
	node := &Node{
		ID:      c.nextNodeID,
		Name:    name,
		Address: address,
		Active:  true,
	}
	c.nodes[node.ID] = node
	log.Printf("Added new node: %s (%s) with ID %d", name, address, node.ID)
	return nil
}

func (c *Cluster) RemoveNode(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.nodes, id)
	// Remove node from all partition replicas
	for _, part := range c.partitions {
		newReplicas := []int{}
		for _, nid := range part.Replicas {
			if nid != id {
				newReplicas = append(newReplicas, nid)
			}
		}
		part.Replicas = newReplicas
		// If removed node was leader, elect new leader
		if part.LeaderID == id {
			if len(newReplicas) > 0 {
				part.LeaderID = newReplicas[0]
			} else {
				part.LeaderID = 0
			}
		}
	}
}

func (c *Cluster) AddPartition() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we have any active nodes available
	activeNodes := 0
	for _, node := range c.nodes {
		if node.Active {
			activeNodes++
		}
	}
	if activeNodes == 0 {
		return fmt.Errorf("no active nodes available to create partition")
	}

	c.nextPartID++
	part := &Partition{
		ID:       c.nextPartID,
		Replicas: []int{},
	}

	// Find the node with the least number of partitions to be the leader
	leaderID := c.findOptimalLeader()
	if leaderID == 0 {
		return fmt.Errorf("no suitable active leader found")
	}

	part.LeaderID = leaderID
	part.Replicas = append(part.Replicas, leaderID)

	// Add one more replica if available
	if activeNodes > 1 {
		replicaID := c.findOptimalReplica(leaderID)
		if replicaID != 0 {
			part.Replicas = append(part.Replicas, replicaID)
		}
	}

	c.partitions[part.ID] = part
	log.Printf("Created new partition %d with leader node %d", part.ID, leaderID)
	return nil
}

// findOptimalLeader returns the ID of the node that should be the leader
// based on the number of partitions it currently leads
func (c *Cluster) findOptimalLeader() int {
	leaderCount := make(map[int]int)

	// Count how many partitions each node leads
	for _, part := range c.partitions {
		leaderCount[part.LeaderID]++
	}

	// Find the active node with the least number of partitions
	minCount := -1
	var optimalLeader int
	for id, node := range c.nodes {
		if !node.Active {
			continue
		}
		count := leaderCount[id]
		if minCount == -1 || count < minCount {
			minCount = count
			optimalLeader = id
		}
	}
	return optimalLeader
}

// findOptimalReplica returns the ID of the best node to be a replica
// that is not the current leader and is active
func (c *Cluster) findOptimalReplica(leaderID int) int {
	replicaCount := make(map[int]int)

	// Count how many partitions each node is a replica for
	for _, part := range c.partitions {
		for _, replicaID := range part.Replicas {
			replicaCount[replicaID]++
		}
	}

	// Find the active node with the least number of replicas that isn't the leader
	minCount := -1
	var optimalReplica int
	for id, node := range c.nodes {
		if !node.Active || id == leaderID {
			continue
		}
		count := replicaCount[id]
		if minCount == -1 || count < minCount {
			minCount = count
			optimalReplica = id
		}
	}
	return optimalReplica
}

func (c *Cluster) RemovePartition(id int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.partitions, id)
}

func (c *Cluster) TransferPartition(pid, newNodeID int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if part, ok := c.partitions[pid]; ok {
		if _, ok := c.nodes[newNodeID]; ok {
			// Add new node to replicas if not already present
			found := false
			for _, nid := range part.Replicas {
				if nid == newNodeID {
					found = true
					break
				}
			}
			if !found {
				part.Replicas = append(part.Replicas, newNodeID)
			}
			// Make new node the leader
			part.LastLeaderID = part.LeaderID
			part.LeaderID = newNodeID
		}
	}
}

func (c *Cluster) ChangeLeader(pid, newLeaderID int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if part, ok := c.partitions[pid]; ok {
		if _, ok := c.nodes[newLeaderID]; ok {
			// Ensure new leader is in replicas
			found := false
			for _, nid := range part.Replicas {
				if nid == newLeaderID {
					found = true
					break
				}
			}
			if !found {
				part.Replicas = append(part.Replicas, newLeaderID)
			}
			part.LastLeaderID = part.LeaderID
			part.LeaderID = newLeaderID
		}
	}
}

func (c *Cluster) GetNodes() map[int]*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a copy of the nodes map
	nodes := make(map[int]*Node)
	for id, node := range c.nodes {
		nodes[id] = node
	}
	return nodes
}

func (c *Cluster) GetPartitions() map[int]*Partition {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create a copy of the partitions map
	partitions := make(map[int]*Partition)
	for id, part := range c.partitions {
		partitions[id] = part
	}
	return partitions
}
