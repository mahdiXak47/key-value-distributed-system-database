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
	ID         int
	Name       string
	Address    string
	Active     bool
	Partitions []int // List of partition IDs this node is responsible for
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
	log.Printf("Cluster: Adding new node - Name: %s, Address: %s", name, address)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate input
	if name == "" || address == "" {
		log.Printf("Cluster: Invalid input - Name and address are required")
		return fmt.Errorf("name and address are required")
	}

	// Check if node with same address already exists
	for _, node := range c.nodes {
		if node.Address == address {
			log.Printf("Cluster: Node with address %s already exists", address)
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
	log.Printf("Cluster: Added new node: %s (%s) with ID %d", name, address, node.ID)

	// Rebalance partitions if needed
	if len(c.partitions) > 0 {
		log.Printf("Cluster: Rebalancing partitions after adding new node")
		c.rebalancePartitions()
	}

	return nil
}

func (c *Cluster) RemoveNode(id int) {
	log.Printf("Cluster: Removing node with ID: %d", id)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if node exists
	if _, exists := c.nodes[id]; !exists {
		log.Printf("Cluster: Node %d does not exist", id)
		return
	}

	// Get node's partitions
	node := c.nodes[id]
	log.Printf("Cluster: Node %d has %d partitions", id, len(node.Partitions))

	// Remove node from partitions
	for _, partID := range node.Partitions {
		if part, exists := c.partitions[partID]; exists {
			log.Printf("Cluster: Removing node %d from partition %d", id, partID)
			// Remove from replicas
			for i, replicaID := range part.Replicas {
				if replicaID == id {
					part.Replicas = append(part.Replicas[:i], part.Replicas[i+1:]...)
					break
				}
			}
			// If node was leader, select new leader
			if part.LeaderID == id {
				log.Printf("Cluster: Node %d was leader for partition %d, selecting new leader", id, partID)
				if len(part.Replicas) > 0 {
					part.LeaderID = part.Replicas[0]
					part.Replicas = part.Replicas[1:]
				} else {
					// No replicas left, partition is now leaderless
					log.Printf("Cluster: Warning - Partition %d has no leader after node removal", partID)
					part.LeaderID = -1
				}
			}
		}
	}

	// Remove node
	log.Printf("Cluster: Deleting node %d from cluster", id)
	delete(c.nodes, id)

	// Rebalance partitions
	if len(c.partitions) > 0 {
		log.Printf("Cluster: Rebalancing partitions after node removal")
		c.rebalancePartitions()
	}

	log.Printf("Cluster: Successfully removed node %d", id)
}

func (c *Cluster) AddPartition() error {
	log.Printf("Cluster: Adding new partition")

	c.mu.Lock()
	// Create new partition
	c.nextPartID++
	partID := c.nextPartID
	partition := &Partition{
		ID:       partID,
		LeaderID: -1,
		Replicas: make([]int, 0),
	}

	log.Printf("Cluster: Created new partition with ID: %d", partID)
	c.partitions[partID] = partition
	c.mu.Unlock() // Release lock before node assignment

	// Assign partition to nodes
	if len(c.nodes) > 0 {
		log.Printf("Cluster: Assigning partition %d to nodes", partID)
		c.assignPartition(partID)
	} else {
		log.Printf("Cluster: Warning - No nodes available to assign partition %d", partID)
	}

	log.Printf("Cluster: Successfully added partition %d", partID)
	return nil
}

func (c *Cluster) RemovePartition(id int) {
	log.Printf("Cluster: Removing partition %d", id)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if partition exists
	if _, exists := c.partitions[id]; !exists {
		log.Printf("Cluster: Partition %d does not exist", id)
		return
	}

	// Remove partition from nodes
	partition := c.partitions[id]
	log.Printf("Cluster: Removing partition %d from leader %d and %d replicas",
		id, partition.LeaderID, len(partition.Replicas))

	// Remove from leader
	if leader, exists := c.nodes[partition.LeaderID]; exists {
		for i, pID := range leader.Partitions {
			if pID == id {
				leader.Partitions = append(leader.Partitions[:i], leader.Partitions[i+1:]...)
				break
			}
		}
	}

	// Remove from replicas
	for _, replicaID := range partition.Replicas {
		if replica, exists := c.nodes[replicaID]; exists {
			for i, pID := range replica.Partitions {
				if pID == id {
					replica.Partitions = append(replica.Partitions[:i], replica.Partitions[i+1:]...)
					break
				}
			}
		}
	}

	// Remove partition
	log.Printf("Cluster: Deleting partition %d from cluster", id)
	delete(c.partitions, id)

	log.Printf("Cluster: Successfully removed partition %d", id)
}

func (c *Cluster) assignPartition(partID int) {
	log.Printf("Cluster: Starting partition assignment for partition %d", partID)

	partition := c.partitions[partID]
	if partition == nil {
		log.Printf("Cluster: Error - Partition %d does not exist", partID)
		return
	}

	// Get available nodes
	availableNodes := make([]int, 0)
	for nodeID, node := range c.nodes {
		if node.Active {
			availableNodes = append(availableNodes, nodeID)
			log.Printf("Cluster: Node %d (%s) is available for partition %d", nodeID, node.Address, partID)
		} else {
			log.Printf("Cluster: Node %d (%s) is not active, skipping for partition %d", nodeID, node.Address, partID)
		}
	}

	if len(availableNodes) == 0 {
		log.Printf("Cluster: Warning - No active nodes available for partition %d", partID)
		return
	}

	// Select leader
	leaderID := availableNodes[0]
	leaderNode := c.nodes[leaderID]
	log.Printf("Cluster: Selected node %d (%s) as leader for partition %d", leaderID, leaderNode.Address, partID)
	partition.LeaderID = leaderID
	c.nodes[leaderID].Partitions = append(c.nodes[leaderID].Partitions, partID)

	// Select replicas (if any nodes left)
	if len(availableNodes) > 1 {
		replicaCount := min(len(availableNodes)-1, 2) // Use at most 2 replicas
		log.Printf("Cluster: Assigning %d replicas for partition %d", replicaCount, partID)

		for i := 1; i <= replicaCount; i++ {
			replicaID := availableNodes[i]
			replicaNode := c.nodes[replicaID]
			partition.Replicas = append(partition.Replicas, replicaID)
			c.nodes[replicaID].Partitions = append(c.nodes[replicaID].Partitions, partID)
			log.Printf("Cluster: Assigned node %d (%s) as replica for partition %d", replicaID, replicaNode.Address, partID)
		}
	} else {
		log.Printf("Cluster: No additional nodes available for replicas of partition %d", partID)
	}

	// Notify nodes about their roles
	log.Printf("Cluster: Notifying nodes about their roles for partition %d", partID)
	currentNodes := c.GetNodes()
	leaderAddress := currentNodes[leaderID].Address
	replicaAddresses := make([]string, 0)
	for _, replicaID := range partition.Replicas {
		if replica, exists := currentNodes[replicaID]; exists {
			replicaAddresses = append(replicaAddresses, replica.Address)
		}
	}

	// Notify leader
	leaderInfo := NodePartitionInfo{
		ID:       partID,
		Role:     "leader",
		Leader:   leaderAddress,
		Replicas: replicaAddresses,
	}
	log.Printf("Cluster: Notifying leader %s about partition %d", leaderAddress, partID)
	go func() {
		if err := c.notifyNode(leaderAddress, leaderInfo); err != nil {
			log.Printf("Cluster: Error notifying leader %s about partition %d: %v", leaderAddress, partID, err)
		}
	}()

	// Notify replicas
	for _, replicaAddr := range replicaAddresses {
		replicaInfo := NodePartitionInfo{
			ID:       partID,
			Role:     "replica",
			Leader:   leaderAddress,
			Replicas: replicaAddresses,
		}
		log.Printf("Cluster: Notifying replica %s about partition %d", replicaAddr, partID)
		go func(addr string, info NodePartitionInfo) {
			if err := c.notifyNode(addr, info); err != nil {
				log.Printf("Cluster: Error notifying replica %s about partition %d: %v", addr, info.ID, err)
			}
		}(replicaAddr, replicaInfo)
	}

	log.Printf("Cluster: Successfully assigned partition %d - Leader: %d (%s), Replicas: %v",
		partID, partition.LeaderID, leaderAddress, replicaAddresses)
}

func (c *Cluster) rebalancePartitions() {
	log.Printf("Cluster: Starting partition rebalancing")

	// Get active nodes
	activeNodes := make([]int, 0)
	for nodeID, node := range c.nodes {
		if node.Active {
			activeNodes = append(activeNodes, nodeID)
		}
	}

	if len(activeNodes) == 0 {
		log.Printf("Cluster: Warning - No active nodes for rebalancing")
		return
	}

	// Calculate target partitions per node
	totalPartitions := len(c.partitions)
	targetPerNode := totalPartitions / len(activeNodes)
	extraPartitions := totalPartitions % len(activeNodes)

	log.Printf("Cluster: Rebalancing %d partitions across %d nodes (target: %d per node, %d extra)",
		totalPartitions, len(activeNodes), targetPerNode, extraPartitions)

	// Clear current assignments
	for _, node := range c.nodes {
		node.Partitions = make([]int, 0)
	}
	for _, partition := range c.partitions {
		partition.LeaderID = -1
		partition.Replicas = make([]int, 0)
	}

	// Reassign partitions
	partID := 0
	for _, nodeID := range activeNodes {
		partitionsForNode := targetPerNode
		if extraPartitions > 0 {
			partitionsForNode++
			extraPartitions--
		}

		log.Printf("Cluster: Assigning %d partitions to node %d", partitionsForNode, nodeID)
		for i := 0; i < partitionsForNode && partID < totalPartitions; i++ {
			c.assignPartition(partID)
			partID++
		}
	}

	log.Printf("Cluster: Completed partition rebalancing")
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
