package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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
	nodes         []*Node
	partitions    []*Partition
	current       int
	mu            sync.RWMutex
	healthCheck   time.Duration
	controllerURL string
	hashRing      *distribution.HashRing
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
		nodes:         make([]*Node, 0),
		partitions:    make([]*Partition, numPartitions),
		healthCheck:   healthCheckInterval,
		controllerURL: controllerURL,
		hashRing:      distribution.NewHashRing(10, numPartitions), // 10 virtual nodes per physical node
	}
}

func (lb *LoadBalancer) AddNode(address string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Check if node already exists
	for _, node := range lb.nodes {
		if node.Address == address {
			return
		}
	}

	// Add node to the hash ring
	lb.hashRing.AddNode(address)

	lb.nodes = append(lb.nodes, &Node{
		Address:     address,
		Active:      true,
		LastChecked: time.Now(),
	})
}

func (lb *LoadBalancer) RemoveNode(address string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Remove node from the hash ring
	lb.hashRing.RemoveNode(address)

	for i, node := range lb.nodes {
		if node.Address == address {
			lb.nodes = append(lb.nodes[:i], lb.nodes[i+1:]...)
			break
		}
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

func (lb *LoadBalancer) getPartitionForKey(key string) int {
	return lb.hashRing.GetPartition(key)
}

func (lb *LoadBalancer) getNodeForKey(key string) *Node {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	// Get the node address from the hash ring
	nodeAddress := lb.hashRing.GetNode(key)
	if nodeAddress == "" {
		return nil
	}

	// Find the node in our nodes list
	for _, node := range lb.nodes {
		if node.Address == nodeAddress && node.Active {
			return node
		}
	}
	return nil
}

func (lb *LoadBalancer) getLeaderForPartition(partitionID int) *Node {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if partitionID < 0 || partitionID >= len(lb.partitions) {
		return nil
	}

	partition := lb.partitions[partitionID]
	if partition == nil {
		return nil
	}

	for _, node := range lb.nodes {
		if node.Address == partition.Leader && node.Active {
			return node
		}
	}
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
	resp, err := http.Get(lb.controllerURL + "/cluster/status")
	if err != nil {
		return fmt.Errorf("failed to get cluster status: %v", err)
	}
	defer resp.Body.Close()

	var controllerResp ControllerResponse
	if err := json.NewDecoder(resp.Body).Decode(&controllerResp); err != nil {
		return fmt.Errorf("failed to decode controller response: %v", err)
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Update nodes
	lb.nodes = make([]*Node, 0, len(controllerResp.Nodes))
	for _, nodeInfo := range controllerResp.Nodes {
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
			lb.partitions[i] = &partition
		}
	}

	return nil
}

func (lb *LoadBalancer) forwardRequest(w http.ResponseWriter, r *http.Request) {
	var req KeyValueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Invalid request body",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Get the target node using consistent hashing
	targetNode := lb.getNodeForKey(req.Key)
	if targetNode == nil {
		// Try to update from controller and retry once
		if err := lb.updateFromController(); err != nil {
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
		targetNode = lb.getNodeForKey(req.Key)
		if targetNode == nil {
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

	// Forward the request to the target node
	reqBody, _ := json.Marshal(req)
	newReq, err := http.NewRequest(r.Method, targetNode.Address+r.URL.Path, bytes.NewBuffer(reqBody))
	if err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Error creating request",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	newReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(newReq)
	if err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Error forwarding request",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}
	defer resp.Body.Close()

	// Copy response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func main() {
	lb := NewLoadBalancer(5*time.Second, "http://localhost:8080", 10) // 10 partitions

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

	// Handle node management
	http.HandleFunc("/node/add", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		address := r.FormValue("address")
		if address != "" {
			lb.AddNode(address)
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Address is required", http.StatusBadRequest)
		}
	})

	http.HandleFunc("/node/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		address := r.FormValue("address")
		if address != "" {
			lb.RemoveNode(address)
			w.WriteHeader(http.StatusOK)
		} else {
			http.Error(w, "Address is required", http.StatusBadRequest)
		}
	})

	log.Println("Starting load balancer on :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
