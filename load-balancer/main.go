package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mahdiXak47/key-value-distributed-system-database/load-balancer/distribution"
)

type Node struct {
	Address     string
	Active      bool
	LastChecked time.Time
	IsLeader    bool
	Partitions  []int
}

type Partition struct {
	ID       int
	Leader   string
	Replicas []string
}

type LoadBalancer struct {
	nodes            []*Node
	partitions       []*Partition
	current          int
	mu               sync.RWMutex
	healthCheck      time.Duration
	controllerURL    string
	partitionManager *distribution.PartitionManager
}

type KeyValueRequest struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type KeyValueResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

type ControllerNodeInfo struct {
	Address    string `json:"address"`
	IsLeader   bool   `json:"is_leader"`
	Partitions []int  `json:"partitions"`
}

type ControllerResponse struct {
	Nodes      []ControllerNodeInfo `json:"nodes"`
	Partitions []Partition          `json:"partitions"`
}

func NewLoadBalancer(healthCheckInterval time.Duration, controllerURL string, numPartitions int) *LoadBalancer {
	return &LoadBalancer{
		nodes:            make([]*Node, 0),
		partitions:       make([]*Partition, numPartitions),
		healthCheck:      healthCheckInterval,
		controllerURL:    controllerURL,
		partitionManager: distribution.NewPartitionManager(numPartitions),
	}
}

func (lb *LoadBalancer) checkNodeHealth(node *Node) bool {
	resp, err := http.Get(node.Address + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (lb *LoadBalancer) startHealthCheck() {
	ticker := time.NewTicker(lb.healthCheck)
	go func() {
		for range ticker.C {
			lb.mu.Lock()
			for _, node := range lb.nodes {
				if time.Since(node.LastChecked) >= lb.healthCheck {
					node.Active = lb.checkNodeHealth(node)
					node.LastChecked = time.Now()
				}
			}
			lb.mu.Unlock()
		}
	}()
}

func (lb *LoadBalancer) getLeaderForPartition(partitionID int) *Node {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	log.Printf("LoadBalancer: Looking up leader for partition %d", partitionID)

	if partitionID < 0 || partitionID >= len(lb.partitions) {
		log.Printf("LoadBalancer: Invalid partition ID %d (range: 0-%d)", partitionID, len(lb.partitions)-1)
		return nil
	}

	partition := lb.partitions[partitionID]
	if partition == nil {
		log.Printf("LoadBalancer: No partition found for ID %d", partitionID)
		return nil
	}

	log.Printf("LoadBalancer: Found partition %d with leader %s", partitionID, partition.Leader)

	for _, node := range lb.nodes {
		if node.Address == partition.Leader && node.Active {
			log.Printf("LoadBalancer: Found active leader node %s for partition %d", node.Address, partitionID)
			return node
		}
	}

	log.Printf("LoadBalancer: No active leader found for partition %d", partitionID)
	return nil
}

func (lb *LoadBalancer) getReplicaForPartition(partitionID int) *Node {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if partitionID < 0 || partitionID >= len(lb.partitions) {
		return nil
	}

	partition := lb.partitions[partitionID]
	if partition == nil || len(partition.Replicas) == 0 {
		return nil
	}

	// Try to find an active replica
	for _, replicaAddr := range partition.Replicas {
		for _, node := range lb.nodes {
			if node.Address == replicaAddr && node.Active {
				return node
			}
		}
	}
	return nil
}

func (lb *LoadBalancer) updateFromController() error {
	log.Printf("LoadBalancer: Updating cluster state from controller at %s", lb.controllerURL)

	req, err := http.NewRequest("GET", lb.controllerURL, nil)
	if err != nil {
		log.Printf("LoadBalancer: Failed to create request to controller: %v", err)
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("LoadBalancer: Failed to get cluster status from controller: %v", err)
		return fmt.Errorf("failed to get cluster status: %v", err)
	}
	defer resp.Body.Close()

	var controllerResp ControllerResponse
	if err := json.NewDecoder(resp.Body).Decode(&controllerResp); err != nil {
		log.Printf("LoadBalancer: Failed to decode controller response: %v", err)
		return fmt.Errorf("failed to decode controller response: %v", err)
	}

	log.Printf("LoadBalancer: Received update from controller - Nodes: %d, Partitions: %d",
		len(controllerResp.Nodes), len(controllerResp.Partitions))

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Update nodes
	lb.nodes = make([]*Node, 0, len(controllerResp.Nodes))
	for _, nodeInfo := range controllerResp.Nodes {
		log.Printf("LoadBalancer: Updating node info - Address: %s, IsLeader: %v, Partitions: %v",
			nodeInfo.Address, nodeInfo.IsLeader, nodeInfo.Partitions)
		lb.nodes = append(lb.nodes, &Node{
			Address:     nodeInfo.Address,
			Active:      true,
			LastChecked: time.Now(),
			IsLeader:    nodeInfo.IsLeader,
			Partitions:  nodeInfo.Partitions,
		})
	}

	// Update partitions
	for i, partition := range controllerResp.Partitions {
		if i < len(lb.partitions) {
			log.Printf("LoadBalancer: Updating partition info - ID: %d, Leader: %s, Replicas: %v",
				partition.ID, partition.Leader, partition.Replicas)
			lb.partitions[i] = &partition
		}
	}

	log.Printf("LoadBalancer: Successfully updated cluster state from controller")
	return nil
}

func (lb *LoadBalancer) forwardRequest(w http.ResponseWriter, r *http.Request) {
	log.Printf("LoadBalancer: Received %s request to %s", r.Method, r.URL.Path)

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("LoadBalancer: Error reading request body: %v", err)
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse JSON request
	var kv struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &kv); err != nil {
		log.Printf("LoadBalancer: Error parsing JSON: %v", err)
		http.Error(w, "Invalid JSON format", http.StatusBadRequest)
		return
	}
	log.Printf("LoadBalancer: Request details - Key: %s, Value length: %d", kv.Key, len(kv.Value))

	// Calculate partition ID
	partitionID := lb.partitionManager.GetPartition(kv.Key)
	log.Printf("LoadBalancer: Calculated partition ID %d for key %s", partitionID, kv.Key)

	// Handle resharding if needed
	if lb.partitionManager.IsResharding() {
		oldPartition, _ := lb.partitionManager.GetOldAndNewPartitions(partitionID)
		log.Printf("LoadBalancer: Resharding in progress. Old partition: %d, New partition: %d", oldPartition, partitionID)
		partitionID = oldPartition
	}

	// Get the leader node for this partition
	targetNode := lb.getLeaderForPartition(partitionID)
	if targetNode == nil {
		log.Printf("LoadBalancer: No leader found for partition %d, attempting to update from controller", partitionID)
		// Try to update from controller and retry once
		if err := lb.updateFromController(); err != nil {
			log.Printf("LoadBalancer: Failed to update from controller: %v", err)
			response := KeyValueResponse{
				Success: false,
				Error:   "No available nodes",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(response)
			return
		}

		// Retry after update
		targetNode = lb.getLeaderForPartition(partitionID)
		if targetNode == nil {
			log.Printf("LoadBalancer: Still no leader found for partition %d after controller update", partitionID)
			response := KeyValueResponse{
				Success: false,
				Error:   "No available nodes after update",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(response)
			return
		}
	}
	log.Printf("LoadBalancer: Selected target node %s for partition %d", targetNode.Address, partitionID)

	// For read operations, we can use replicas if configured
	if r.Method == http.MethodGet {
		// Try to get a replica first
		replicaNode := lb.getReplicaForPartition(partitionID)
		if replicaNode != nil {
			log.Printf("LoadBalancer: Using replica node %s for read operation", replicaNode.Address)
			targetNode = replicaNode
		}
	}

	// Forward the request to the target node
	reqBody, _ := json.Marshal(kv)

	// Construct full URL for the DB node endpoint
	forwardURL := targetNode.Address + r.URL.Path
	if !strings.HasPrefix(forwardURL, "http://") && !strings.HasPrefix(forwardURL, "https://") {
		forwardURL = "http://" + forwardURL
	}
	log.Printf("LoadBalancer: Forwarding request to %s", forwardURL)

	newReq, err := http.NewRequest(r.Method, forwardURL, bytes.NewBuffer(reqBody))
	if err != nil {
		log.Printf("LoadBalancer: Error creating request to forward to DB node %s: %v", targetNode.Address, err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Error creating forwarded request",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Copy headers
	for name, values := range r.Header {
		for _, value := range values {
			newReq.Header.Add(name, value)
		}
	}
	// Add partition ID header
	newReq.Header.Set("X-Partition-ID", strconv.Itoa(partitionID))

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(newReq)
	if err != nil {
		log.Printf("LoadBalancer: Error forwarding request to DB node %s: %v", targetNode.Address, err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Error forwarding request to backend service",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(response)
		return
	}
	defer resp.Body.Close()

	log.Printf("LoadBalancer: Received response from DB node with status: %d", resp.StatusCode)
	// Copy response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// Handle partition resizing
func (lb *LoadBalancer) handlePartitionResize(newNumPartitions int) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Start resharding process
	lb.partitionManager.StartResharding(newNumPartitions)

	// Update partitions array
	newPartitions := make([]*Partition, newNumPartitions)
	copy(newPartitions, lb.partitions)
	lb.partitions = newPartitions

	// In a real implementation, you would:
	// 1. Start data migration from old partitions to new ones
	// 2. Update routing tables
	// 3. Complete resharding when migration is done
}

func main() {
	lb := NewLoadBalancer(5*time.Second, "http://localhost:8080/cluster/status", 10) // Point to the correct /cluster/status endpoint

	// Start health check
	lb.startHealthCheck()

	// Start periodic controller updates
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for range ticker.C {
			if err := lb.updateFromController(); err != nil {
				log.Printf("Failed to update from controller: %v", err)
			}
		}
	}()

	// Handle key-value operations
	http.HandleFunc("/set", lb.forwardRequest)
	http.HandleFunc("/get", lb.forwardRequest)
	http.HandleFunc("/delete", lb.forwardRequest)

	log.Println("Starting load balancer on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
