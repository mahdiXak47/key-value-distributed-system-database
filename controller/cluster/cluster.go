package cluster

import (
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

func (c *Cluster) AddNode(name, address string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextNodeID++
	node := &Node{
		ID:      c.nextNodeID,
		Name:    name,
		Address: address,
		Active:  true,
	}
	c.nodes[node.ID] = node
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

func (c *Cluster) AddPartition() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextPartID++
	part := &Partition{
		ID:       c.nextPartID,
		Replicas: []int{},
	}

	// Assign to first available node as leader
	for _, node := range c.nodes {
		part.LeaderID = node.ID
		part.Replicas = append(part.Replicas, node.ID)
		break
	}

	c.partitions[part.ID] = part
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
