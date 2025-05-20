package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
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

// PartitionInfo mirrors the struct expected by the db-node's /partition/assign
// This is also defined in db-node/main.go, consider a shared types package in a real project.
type NodePartitionInfo struct {
	ID       int      `json:"id"`
	Role     string   `json:"role"`     // "leader" or "replica"
	Leader   string   `json:"leader"`   // Address of the leader
	Replicas []string `json:"replicas"` // Addresses of all replicas
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
		changedPartitionIDsThisCycle := make(map[int]struct{}) // Store partition IDs that had a leader change in this cycle

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
		for partID, part := range c.partitions {
			if leader, ok := c.nodes[part.LeaderID]; (ok && !leader.Active) || !ok { // Leader is down or leader ID is invalid
				oldLeader := part.LeaderID
				log.Printf("StartHealthChecks: Detected leader failure for partition %d (old leader ID: %d). Attempting to find new leader.", partID, oldLeader)
				foundNewLeader := false
				for _, replicaNodeID := range part.Replicas {
					if node, nodeOK := c.nodes[replicaNodeID]; nodeOK && node.Active && replicaNodeID != oldLeader {
						part.LeaderID = replicaNodeID
						part.LastLeaderID = oldLeader
						foundNewLeader = true
						changedPartitionIDsThisCycle[partID] = struct{}{}
						log.Printf("StartHealthChecks: Partition %d new leader is Node %d (Address: %s) due to old leader %d failure.", partID, replicaNodeID, node.Address, oldLeader)
						break // Found new leader for this partition
					}
				}
				if !foundNewLeader {
					log.Printf("StartHealthChecks: Partition %d has no active replicas to promote after leader %d failure.", partID, oldLeader)
					if part.LeaderID == oldLeader { // If no new leader was found, and old leader was indeed the one failing
						// part.LeaderID = 0 // Optional: mark as leaderless if strictly needed, or keep old if it might come back
						// If we set LeaderID to 0, changedPartitionIDsThisCycle should also capture this if nodes need to be told "no leader"
						changedPartitionIDsThisCycle[partID] = struct{}{}
					}
				}
			} // End of leader failure check for a partition

			// Handling for when a LastLeaderID exists (e.g. old leader came back up)
			// This current logic just resets LastLeaderID. If re-electing the old leader or other actions are needed,
			// that would also need to populate changedPartitionIDsThisCycle.
			if part.LastLeaderID != 0 {
				if oldNode, ok := c.nodes[part.LastLeaderID]; ok && oldNode.Active {
					log.Printf("StartHealthChecks: Old leader Node %d (Address: %s) for partition %d is back online. Current leader is Node %d.", part.LastLeaderID, oldNode.Address, partID, part.LeaderID)
					// Potentially, if old leader came back, and it was part of replicas, it just rejoins as replica.
					// If a policy was to re-promote it, that logic would go here and add to changedPartitionIDsThisCycle.
					part.LastLeaderID = 0 // Reset, as we've noted it's back.
				}
			}
		} // End of loop through partitions
		c.mu.Unlock()

		// Perform notifications for leader changes detected during this health check cycle
		if len(changedPartitionIDsThisCycle) > 0 {
			currentNodesView := c.GetNodes()           // Get a consistent view of nodes
			currentPartitionsView := c.GetPartitions() // Get a consistent view of partitions

			for partID := range changedPartitionIDsThisCycle {
				p, pExists := currentPartitionsView[partID]
				if !pExists {
					log.Printf("StartHealthChecks: Partition %d (marked for change notification) not found in current view.", partID)
					continue
				}

				leaderAddress := ""
				if p.LeaderID != 0 {
					if lNode, exists := currentNodesView[p.LeaderID]; exists && lNode.Active { // Check if new leader is known and active
						leaderAddress = lNode.Address
					} else {
						log.Printf("StartHealthChecks: New leader Node %d for partition %d is not active or not found during notification. Notifying with potentially empty leader.", p.LeaderID, p.ID)
						// Leader might be 0 if no active replica was found. DB nodes should handle this.
					}
				}

				allReplicaNodeAddresses := []string{}
				nodesToNotifyForThisPartition := make(map[string]NodePartitionInfo) // nodeAddress -> assignmentInfo

				for _, replicaNodeID := range p.Replicas {
					node, nodeExists := currentNodesView[replicaNodeID]
					if !nodeExists { // Don't check for node.Active here, node should be notified even if controller thinks it *just* went down
						log.Printf("StartHealthChecks: Replica node %d (ID) for partition %d not found in currentNodesView. Skipping notification for this replica.", replicaNodeID, p.ID)
						continue
					}
					allReplicaNodeAddresses = append(allReplicaNodeAddresses, node.Address)

					role := "replica"
					if replicaNodeID == p.LeaderID {
						role = "leader"
					}
					nodesToNotifyForThisPartition[node.Address] = NodePartitionInfo{
						ID:       p.ID,
						Role:     role,
						Leader:   leaderAddress,
						Replicas: nil, // Placeholder, will be filled with allReplicaNodeAddresses
					}
				}

				// Fill replica addresses for each notification for this partition
				for addr, info := range nodesToNotifyForThisPartition {
					info.Replicas = allReplicaNodeAddresses
					nodesToNotifyForThisPartition[addr] = info
				}

				for nodeAddrToNotify, assignmentInfo := range nodesToNotifyForThisPartition {
					go func(addr string, pInfo NodePartitionInfo) {
						if err := c.notifyNode(addr, pInfo); err != nil {
							log.Printf("StartHealthChecks: Failed to notify node %s about partition %d leader change: %v", addr, pInfo.ID, err)
						}
					}(nodeAddrToNotify, assignmentInfo)
				}
			}
		}
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
	// defer c.mu.Unlock() // Unlock will be handled after notifications

	removedNode, nodeExists := c.nodes[id]
	if !nodeExists {
		c.mu.Unlock()
		log.Printf("RemoveNode: Node %d not found.", id)
		return
	}
	log.Printf("Removing node %d (%s) from cluster.", id, removedNode.Address)
	delete(c.nodes, id)

	partitionsToNotify := make(map[int]bool) // Set of partition IDs that were affected

	// Remove node from all partition replicas and handle leader changes
	for partID, part := range c.partitions {
		wasLeader := (part.LeaderID == id)
		wasReplica := false
		newReplicas := []int{}
		for _, nid := range part.Replicas {
			if nid != id {
				newReplicas = append(newReplicas, nid)
			} else {
				wasReplica = true
			}
		}

		if wasLeader || wasReplica {
			partitionsToNotify[partID] = true
			part.Replicas = newReplicas
			log.Printf("Node %d removed from replicas of partition %d. New replica set: %v", id, partID, newReplicas)

			if wasLeader {
				part.LastLeaderID = id
				if len(newReplicas) > 0 {
					// Simplistic: pick the first remaining replica as the new leader
					// A more robust system might use a defined election or check node health/load.
					part.LeaderID = newReplicas[0]
					log.Printf("Node %d was leader of partition %d. New leader is Node %d.", id, partID, part.LeaderID)
				} else {
					part.LeaderID = 0 // No leader left
					log.Printf("Node %d was leader of partition %d. No replicas left to promote.", id, partID)
				}
			}
		} // else: removed node was not part of this partition, no change to part.Replicas or part.LeaderID
	}
	c.mu.Unlock() // Release lock before network calls

	// Notify nodes of affected partitions
	if len(partitionsToNotify) > 0 {
		currentNodes := c.GetNodes() // Get a fresh copy of nodes (excluding the removed one)
		currentPartitions := c.GetPartitions()

		for partID := range partitionsToNotify {
			affectedPartition, pExists := currentPartitions[partID]
			if !pExists {
				log.Printf("RemoveNode: Partition %d (marked for notification) not found after node removal.", partID)
				continue
			}

			leaderAddress := ""
			if affectedPartition.LeaderID != 0 { // Check if there is still a leader
				if lNode, exists := currentNodes[affectedPartition.LeaderID]; exists {
					leaderAddress = lNode.Address
				} else {
					log.Printf("RemoveNode: Leader node %d not found for partition %d during notification.", affectedPartition.LeaderID, partID)
					// This partition might be temporarily leaderless or its leader is not in currentNodes (should not happen if logic is correct)
					// For now, we proceed, and replicas will get an empty leader address if so.
				}
			}

			allReplicaNodeAddresses := []string{}
			nodesToNotifyForThisPartition := make(map[string]NodePartitionInfo) // nodeAddress -> assignmentInfo

			for _, replicaNodeID := range affectedPartition.Replicas {
				node, nodeExists := currentNodes[replicaNodeID]
				if !nodeExists {
					log.Printf("RemoveNode: Replica node %d (ID) not found for partition %d during notification. It might be the one just removed or another issue.", replicaNodeID, partID)
					continue
				}
				allReplicaNodeAddresses = append(allReplicaNodeAddresses, node.Address)

				role := "replica"
				if replicaNodeID == affectedPartition.LeaderID {
					role = "leader"
				}
				nodesToNotifyForThisPartition[node.Address] = NodePartitionInfo{
					ID:       affectedPartition.ID,
					Role:     role,
					Leader:   leaderAddress,
					Replicas: nil, // Placeholder
				}
			}

			// Fill replica addresses for each notification for this partition
			for addr, info := range nodesToNotifyForThisPartition {
				info.Replicas = allReplicaNodeAddresses
				nodesToNotifyForThisPartition[addr] = info
			}

			for nodeAddrToNotify, assignmentInfo := range nodesToNotifyForThisPartition {
				go func(addr string, pInfo NodePartitionInfo) {
					if err := c.notifyNode(addr, pInfo); err != nil {
						log.Printf("RemoveNode: Failed to notify node %s about partition %d change after node removal: %v", addr, pInfo.ID, err)
					}
				}(nodeAddrToNotify, assignmentInfo)
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

	// Unlock before sending notifications to avoid holding lock during network calls
	c.mu.Unlock()

	// Notify relevant nodes
	// leaderNode := c.nodes[part.LeaderID] // This was unused, removing it.

	// It's safer to re-acquire read lock to get node details if GetNodes is used,
	// or pass necessary node details to a notification function that doesn't need cluster lock.
	// For simplicity here, assuming c.nodes[id] access is okay after initial lock release if AddNode/RemoveNode take full lock.
	// A better approach would be to get all necessary data under lock, then release, then notify.

	// Let's get all node addresses involved in this partition
	// Need to read c.nodes and c.partitions again, preferably under RLock if not already done
	// For now, assuming part and leaderNode are stable after unlock for this example flow.
	// This section needs careful thought on locking for a production system.

	allNodesInPartition := make(map[int]*Node)
	involvedNodeAddresses := make(map[int]string) // nodeID to address

	currentNodes := c.GetNodes()           // This gets a copy under RLock
	currentPartitions := c.GetPartitions() // This gets a copy under RLock

	specificPartition, ok := currentPartitions[part.ID]
	if !ok {
		log.Printf("Partition %d not found after creation for notification, this should not happen.", part.ID)
		return fmt.Errorf("partition %d not found after creation for notification", part.ID)
	}

	var leaderAddress string
	if lNode, exists := currentNodes[specificPartition.LeaderID]; exists {
		leaderAddress = lNode.Address
	} else {
		log.Printf("Leader node %d not found for partition %d during notification.", specificPartition.LeaderID, specificPartition.ID)
		// Cannot proceed without leader address
		return fmt.Errorf("leader node %d not found for partition %d", specificPartition.LeaderID, specificPartition.ID)
	}

	replicaAddresses := []string{}
	for _, replicaNodeID := range specificPartition.Replicas {
		if rNode, exists := currentNodes[replicaNodeID]; exists {
			allNodesInPartition[replicaNodeID] = rNode
			involvedNodeAddresses[replicaNodeID] = rNode.Address
			replicaAddresses = append(replicaAddresses, rNode.Address)
		} else {
			log.Printf("Replica node %d not found for partition %d during notification.", replicaNodeID, specificPartition.ID)
			// Continue notifying other replicas
		}
	}

	for nodeID, node := range allNodesInPartition {
		role := "replica"
		if nodeID == specificPartition.LeaderID {
			role = "leader"
		}
		assignmentInfo := NodePartitionInfo{
			ID:       specificPartition.ID,
			Role:     role,
			Leader:   leaderAddress,    // Address of the current leader
			Replicas: replicaAddresses, // Addresses of all replicas in this partition
		}
		// Asynchronously notify or handle errors carefully
		go func(addr string, pInfo NodePartitionInfo) {
			if err := c.notifyNode(addr, pInfo); err != nil {
				log.Printf("Failed to notify node %s about partition %d: %v", addr, pInfo.ID, err)
				// Further error handling: retry, mark node as having stale config, etc.
			}
		}(node.Address, assignmentInfo)
	}

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
	// defer c.mu.Unlock() // Unlock will be handled after notifications

	part, okPart := c.partitions[pid]
	if !okPart {
		c.mu.Unlock()
		log.Printf("TransferPartition: Partition %d not found.", pid)
		return
	}
	newNode, okNode := c.nodes[newNodeID]
	if !okNode {
		c.mu.Unlock()
		log.Printf("TransferPartition: Target node %d not found.", newNodeID)
		return
	}
	if !newNode.Active { // Optionally check if new leader is active
		c.mu.Unlock()
		log.Printf("TransferPartition: Target node %d is not active.", newNodeID)
		return
	}

	// Update internal state
	part.LastLeaderID = part.LeaderID
	part.LeaderID = newNodeID // The new node becomes the leader

	// Add new node to replicas if not already present
	isNewNodeInReplicas := false
	for _, replicaID := range part.Replicas {
		if replicaID == newNodeID {
			isNewNodeInReplicas = true
			break
		}
	}
	if !isNewNodeInReplicas {
		part.Replicas = append(part.Replicas, newNodeID)
	}

	log.Printf("Internal state updated: Partition %d transferred to Node %d. Leader: %d, Replicas: %v", pid, newNodeID, part.LeaderID, part.Replicas)
	c.mu.Unlock() // Release lock before network calls

	// Notify all nodes in the partition about the change
	currentNodes := c.GetNodes()
	currentPartition, pExists := c.GetPartitions()[pid]
	if !pExists {
		log.Printf("TransferPartition: Partition %d disappeared before notification.", pid)
		return
	}

	leaderAddress := ""
	if lNode, exists := currentNodes[currentPartition.LeaderID]; exists {
		leaderAddress = lNode.Address
	} else {
		log.Printf("TransferPartition: Critical - Leader node %d (address) not found for partition %d.", currentPartition.LeaderID, pid)
		return
	}

	allReplicaNodeAddresses := []string{}
	nodesToNotify := make(map[string]NodePartitionInfo) // nodeAddress -> assignmentInfo

	for _, replicaNodeID := range currentPartition.Replicas {
		node, nodeExists := currentNodes[replicaNodeID]
		if !nodeExists {
			log.Printf("TransferPartition: Replica node %d not found for partition %d during notification prep.", replicaNodeID, pid)
			continue
		}
		allReplicaNodeAddresses = append(allReplicaNodeAddresses, node.Address)

		role := "replica"
		if replicaNodeID == currentPartition.LeaderID {
			role = "leader"
		}
		// Use node.Address as the key for nodesToNotify
		nodesToNotify[node.Address] = NodePartitionInfo{
			ID:       currentPartition.ID,
			Role:     role,
			Leader:   leaderAddress,
			Replicas: nil, // Placeholder, will be filled next
		}
	}

	// Fill replica addresses for each notification
	// Ensure allReplicaNodeAddresses is populated before this loop
	for addr, info := range nodesToNotify {
		info.Replicas = allReplicaNodeAddresses // All nodes get the same full list of replica addresses
		nodesToNotify[addr] = info              // Update the map with the completed info struct
	}

	for nodeAddrToNotify, assignmentInfo := range nodesToNotify {
		go func(addr string, pInfo NodePartitionInfo) {
			if err := c.notifyNode(addr, pInfo); err != nil {
				log.Printf("TransferPartition: Failed to notify node %s about partition %d change: %v", addr, pInfo.ID, err)
			}
		}(nodeAddrToNotify, assignmentInfo) // Pass the correct nodeAddrToNotify here
	}
}

func (c *Cluster) ChangeLeader(pid, newLeaderID int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	part, okPart := c.partitions[pid]
	if !okPart {
		log.Printf("ChangeLeader: Partition %d not found.", pid)
		return
	}
	newNode, okNode := c.nodes[newLeaderID]
	if !okNode {
		log.Printf("ChangeLeader: New leader node %d not found.", newLeaderID)
		return
	}
	if !newNode.Active {
		log.Printf("ChangeLeader: New leader node %d is not active.", newLeaderID)
		return
	}

	// Update internal state
	part.LastLeaderID = part.LeaderID
	part.LeaderID = newLeaderID

	// Ensure new leader is in replicas list
	isNewLeaderInReplicas := false
	for _, replicaID := range part.Replicas {
		if replicaID == newLeaderID {
			isNewLeaderInReplicas = true
			break
		}
	}
	if !isNewLeaderInReplicas {
		part.Replicas = append(part.Replicas, newLeaderID)
	}

	log.Printf("Internal state updated: Partition %d new leader is Node %d. Replicas: %v", pid, newLeaderID, part.Replicas)

	// Notify all nodes in the partition about the change
	// Collect necessary info first (similar to AddPartition, get current node addresses)
	currentNodes := c.GetNodes()
	currentPartition, pExists := c.GetPartitions()[pid]
	if !pExists {
		log.Printf("ChangeLeader: Partition %d disappeared before notification.", pid)
		return
	}

	leaderAddress := ""
	if lNode, exists := currentNodes[currentPartition.LeaderID]; exists {
		leaderAddress = lNode.Address
	} else {
		log.Printf("ChangeLeader: Critical - Leader node %d (address) not found for partition %d during notification.", currentPartition.LeaderID, pid)
		return
	}

	allReplicaNodeAddresses := []string{}
	nodesToNotify := make(map[string]NodePartitionInfo) // nodeAddress -> assignmentInfo

	for _, replicaNodeID := range currentPartition.Replicas {
		node, nodeExists := currentNodes[replicaNodeID]
		if !nodeExists {
			log.Printf("ChangeLeader: Replica node %d not found for partition %d during notification prep.", replicaNodeID, pid)
			continue
		}
		allReplicaNodeAddresses = append(allReplicaNodeAddresses, node.Address)

		role := "replica"
		if replicaNodeID == currentPartition.LeaderID {
			role = "leader"
		}
		nodesToNotify[node.Address] = NodePartitionInfo{
			ID:       currentPartition.ID,
			Role:     role,
			Leader:   leaderAddress,
			Replicas: nil, // Placeholder, will be filled next
		}
	}

	// Fill replica addresses for each notification
	for addr, info := range nodesToNotify {
		info.Replicas = allReplicaNodeAddresses // All nodes get the same full list of replica addresses
		nodesToNotify[addr] = info
	}

	for nodeAddr, assignmentInfo := range nodesToNotify {
		go func(addr string, pInfo NodePartitionInfo) {
			if err := c.notifyNode(addr, pInfo); err != nil {
				log.Printf("ChangeLeader: Failed to notify node %s about partition %d change: %v", addr, pInfo.ID, err)
			}
		}(nodeAddr, assignmentInfo)
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

// notifyNode tells a specific db-node about its assignment for a given partition.
// nodeAddress should be the address (host:port) of the db-node.
// partInfo contains all necessary details for the assignment.
func (c *Cluster) notifyNode(nodeAddress string, partInfo NodePartitionInfo) error {
	jsonData, err := json.Marshal(partInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal partition info for node %s: %v", nodeAddress, err)
	}

	// Ensure address starts with http://
	if !strings.HasPrefix(nodeAddress, "http://") && !strings.HasPrefix(nodeAddress, "https://") {
		nodeAddress = "http://" + nodeAddress
	}
	url := fmt.Sprintf("%s/partition/assign", nodeAddress)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error sending partition assignment to node %s (partition %d): %v", nodeAddress, partInfo.ID, err)
		return fmt.Errorf("failed to create request to %s for partition %d: %v", url, partInfo.ID, err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		// Log network errors or node being down without necessarily stopping controller operations for other nodes.
		log.Printf("Error sending partition assignment to node %s (partition %d): %v", nodeAddress, partInfo.ID, err)
		return fmt.Errorf("failed to send request to %s for partition %d: %v", url, partInfo.ID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// Log application-level errors from the node.
		log.Printf("Node %s returned status %d for partition assignment (partition %d)", nodeAddress, resp.StatusCode, partInfo.ID)
		return fmt.Errorf("node %s returned status %d for partition assignment (partition %d)", nodeAddress, resp.StatusCode, partInfo.ID)
	}

	log.Printf("Successfully notified node %s about partition %d (Role: %s, Leader: %s)", nodeAddress, partInfo.ID, partInfo.Role, partInfo.Leader)
	return nil
}
