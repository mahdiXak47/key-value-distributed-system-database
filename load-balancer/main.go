package main

import (
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
	partitions       map[int]*Partition
	current          int
	mu               sync.RWMutex
	healthCheck      time.Duration
	controllerURL    string
	partitionManager *distribution.PartitionManager
	usePartitioning  bool // New flag to control partitioning
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
		partitions:       make(map[int]*Partition),
		healthCheck:      healthCheckInterval,
		controllerURL:    controllerURL,
		partitionManager: distribution.NewPartitionManager(numPartitions),
		usePartitioning:  false, // Start with simple round-robin
	}
}

// New method for round-robin node selection
func (lb *LoadBalancer) getNextAvailableNode() *Node {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	if len(lb.nodes) == 0 {
		return nil
	}

	// Try to find an active node starting from current
	startIndex := lb.current
	for i := 0; i < len(lb.nodes); i++ {
		index := (startIndex + i) % len(lb.nodes)
		if lb.nodes[index].Active {
			lb.current = (index + 1) % len(lb.nodes)
			return lb.nodes[index]
		}
	}

	return nil
}

func (lb *LoadBalancer) checkNodeHealth(node *Node) bool {
	// Fix URL formatting
	healthURL := node.Address
	if !strings.HasPrefix(healthURL, "http://") && !strings.HasPrefix(healthURL, "https://") {
		healthURL = "http://" + healthURL
	}
	healthURL = fmt.Sprintf("%s/health", healthURL)

	resp, err := http.Get(healthURL)
	if err != nil {
		log.Printf("Health check failed for node %s: %v", node.Address, err)
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
				wasActive := node.Active
				node.Active = lb.checkNodeHealth(node)
				node.LastChecked = time.Now()
				if wasActive != node.Active {
					log.Printf("Node %s status changed: active=%v", node.Address, node.Active)
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

	partition, exists := lb.partitions[partitionID]
	if !exists {
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

	partition, exists := lb.partitions[partitionID]
	if !exists || len(partition.Replicas) == 0 {
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
	lb.partitions = make(map[int]*Partition)
	for _, p := range controllerResp.Partitions {
		log.Printf("LoadBalancer: Updating partition info - ID: %d, Leader: %s, Replicas: %v",
			p.ID, p.Leader, p.Replicas)
		// Create a new partition to avoid the loop variable issue
		partition := Partition{
			ID:       p.ID,
			Leader:   p.Leader,
			Replicas: p.Replicas,
		}
		lb.partitions[p.ID] = &partition
	}

	log.Printf("LoadBalancer: Successfully updated cluster state from controller")
	return nil
}

func (lb *LoadBalancer) forwardRequest(w http.ResponseWriter, r *http.Request) {
	var targetNode *Node
	var key string

	if r.Method == "GET" {
		// For GET requests, get key from query parameters
		key = r.URL.Query().Get("key")
		if key == "" {
			log.Printf("LoadBalancer: No key provided in query parameters")
			response := KeyValueResponse{
				Success: false,
				Error:   "Key is required",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(response)
			return
		}
		log.Printf("LoadBalancer: Received GET request for key: %s", key)
	} else {
		// For POST/DELETE requests, read from body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("LoadBalancer: Error reading request body: %v", err)
			response := KeyValueResponse{
				Success: false,
				Error:   "Error reading request",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(response)
			return
		}
		r.Body.Close()

		var kv KeyValueRequest
		if err := json.Unmarshal(body, &kv); err != nil {
			log.Printf("LoadBalancer: Error parsing JSON: %v", err)
			response := KeyValueResponse{
				Success: false,
				Error:   "Invalid JSON format",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(response)
			return
		}
		key = kv.Key
		log.Printf("LoadBalancer: Received %s request for key: %s", r.Method, key)
	}

	// Get partition ID for the key
	partitionID := 1 // For now, use partition 1 for all keys
	log.Printf("LoadBalancer: Using partition %d for key %s", partitionID, key)

	// Get leader node for the partition
	targetNode = lb.getLeaderForPartition(partitionID)
	if targetNode == nil {
		log.Printf("LoadBalancer: No leader found for partition %d", partitionID)
		response := KeyValueResponse{
			Success: false,
			Error:   fmt.Sprintf("No leader found for partition %d", partitionID),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(response)
		return
	}

	log.Printf("LoadBalancer: Forwarding request to node %s", targetNode.Address)

	// Create new request to forward - handle URL properly
	nodeAddr := targetNode.Address
	if !strings.HasPrefix(nodeAddr, "http://") && !strings.HasPrefix(nodeAddr, "https://") {
		nodeAddr = "http://" + nodeAddr
	}

	var newReq *http.Request
	var err error

	if r.Method == "GET" {
		// For GET requests, forward with query parameters
		url := fmt.Sprintf("%s%s?key=%s", nodeAddr, r.URL.Path, key)
		newReq, err = http.NewRequest(r.Method, url, nil)
	} else {
		// For POST/DELETE requests, forward with body
		url := fmt.Sprintf("%s%s", nodeAddr, r.URL.Path)
		newReq, err = http.NewRequest(r.Method, url, r.Body)
	}

	if err != nil {
		log.Printf("LoadBalancer: Error creating forward request: %v", err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Error creating forward request",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Copy headers from original request
	for name, values := range r.Header {
		for _, value := range values {
			newReq.Header.Add(name, value)
		}
	}

	// Add partition ID header
	newReq.Header.Set("X-Partition-ID", strconv.Itoa(partitionID))
	newReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(newReq)
	if err != nil {
		log.Printf("LoadBalancer: Error forwarding request: %v", err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Error forwarding request to node",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(response)
		return
	}
	defer resp.Body.Close()

	// Read the response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("LoadBalancer: Error reading response body: %v", err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Error reading response from node",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Try to parse the response as JSON
	var nodeResponse KeyValueResponse
	if err := json.Unmarshal(respBody, &nodeResponse); err != nil {
		// If we can't parse the response as JSON, wrap the raw response in our format
		log.Printf("LoadBalancer: Error parsing node response as JSON: %v", err)
		nodeResponse = KeyValueResponse{
			Success: resp.StatusCode >= 200 && resp.StatusCode < 300,
			Value:   string(respBody),
		}
		if !nodeResponse.Success {
			nodeResponse.Error = string(respBody)
		}
	}

	// Send the response back to the client
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	json.NewEncoder(w).Encode(nodeResponse)
}

// Handle partition resizing
func (lb *LoadBalancer) handlePartitionResize(newNumPartitions int) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Start resharding process
	lb.partitionManager.StartResharding(newNumPartitions)

	// Update partitions array
	newPartitions := make(map[int]*Partition)
	for _, partition := range lb.partitions {
		newPartitions[partition.ID] = partition
	}
	lb.partitions = newPartitions

	// In a real implementation, you would:
	// 1. Start data migration from old partitions to new ones
	// 2. Update routing tables
	// 3. Complete resharding when migration is done
}

func main() {
	lb := NewLoadBalancer(5*time.Second, "http://localhost:8080/cluster/status", 10)

	// Start health check
	lb.startHealthCheck()

	// Start periodic controller updates
	go func() {
		ticker := time.NewTicker(5 * time.Second)
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

	log.Printf("Starting load balancer on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
