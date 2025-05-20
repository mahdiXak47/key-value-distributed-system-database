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
	"time"

	"github.com/mahdiXak47/key-value-distributed-system-database/db-node/config"
	"github.com/mahdiXak47/key-value-distributed-system-database/db-node/lsm"
	"github.com/mahdiXak47/key-value-distributed-system-database/db-node/partition"
)

var (
	dbStorage        = partition.NewStorage(0)
	partitionManager = partition.NewManager()
	selfAddress      string
	appConfig        *config.Config
)

type HealthStatus struct {
	Status       string `json:"status"`
	MemTableSize int    `json:"memTableSize"`
	Levels       int    `json:"levels"`
	Partitions   []int  `json:"partitions"`
}

type KeyValueResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
}

type PartitionInfo struct {
	ID       int      `json:"id"`
	Role     string   `json:"role"`
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
}

// ReplicationPayload defines the structure for data sent to replicas
type ReplicationPayload struct {
	PartitionID int64          `json:"partition_id"`
	WALEntries  []lsm.LogEntry `json:"wal_entries"` // Assuming lsm.LogEntry is defined in the lsm package
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	log.Printf("DB-Node: Received health check request from %s", r.RemoteAddr)

	metrics := dbStorage.GetMetrics()
	status := HealthStatus{
		Status:       "OK",
		MemTableSize: metrics["memTableSize"].(int),
		Levels:       metrics["levels"].(int),
		Partitions:   partitionManager.GetPartitionIDs(),
	}
	log.Printf("DB-Node: Health status - MemTableSize: %d, Levels: %d, Partitions: %v",
		status.MemTableSize, status.Levels, status.Partitions)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	log.Printf("DB-Node: Received SET request from %s", r.RemoteAddr)
	log.Printf("DB-Node: Request headers: %v", r.Header)

	if r.Method != http.MethodPost {
		log.Printf("DB-Node: Invalid method %s", r.Method)
		response := KeyValueResponse{
			Success: false,
			Error:   "Method not allowed",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("DB-Node: Error reading request body: %v", err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Error reading request body",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}
	defer r.Body.Close()

	// Parse JSON request
	var kv struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &kv); err != nil {
		log.Printf("DB-Node: Error parsing JSON: %v", err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Invalid JSON format",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}
	log.Printf("DB-Node: Parsed key-value pair - Key: %s, Value length: %d", kv.Key, len(kv.Value))

	// Validate input
	if kv.Key == "" {
		log.Printf("DB-Node: Empty key received")
		response := KeyValueResponse{
			Success: false,
			Error:   "Key cannot be empty",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Get partition ID from header
	partitionIDStr := r.Header.Get("X-Partition-ID")
	if partitionIDStr == "" {
		log.Printf("DB-Node: Missing X-Partition-ID header")
		response := KeyValueResponse{
			Success: false,
			Error:   "Partition ID is required",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		log.Printf("DB-Node: Invalid partition ID %s: %v", partitionIDStr, err)
		response := KeyValueResponse{
			Success: false,
			Error:   "Invalid partition ID",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}
	log.Printf("DB-Node: Processing request for partition %d", partitionID)

	// Check if this node is responsible for the partition
	if !partitionManager.HasPartition(partitionID) {
		log.Printf("DB-Node: Node does not have partition %d", partitionID)
		response := KeyValueResponse{
			Success: false,
			Error:   "Partition not found",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Check if this node is the leader for the partition
	if !partitionManager.IsLeader(partitionID) {
		log.Printf("DB-Node: Node is not leader for partition %d, forwarding to leader", partitionID)
		// Forward to leader
		p := partitionManager.GetPartition(partitionID)
		if p == nil {
			log.Printf("DB-Node: Failed to get partition info for %d", partitionID)
			response := KeyValueResponse{
				Success: false,
				Error:   "Partition not found",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(response)
			return
		}

		// Forward request to leader
		leaderURL := p.Leader + r.URL.Path
		log.Printf("DB-Node: Forwarding to leader at %s", leaderURL)
		resp, err := http.Post(leaderURL, "application/json", bytes.NewBuffer(body))
		if err != nil {
			log.Printf("DB-Node: Error forwarding to leader: %v", err)
			response := KeyValueResponse{
				Success: false,
				Error:   "Error forwarding to leader",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(response)
			return
		}
		defer resp.Body.Close()

		log.Printf("DB-Node: Received response from leader with status %d", resp.StatusCode)
		// Copy response from leader
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	log.Printf("DB-Node: Node is leader for partition %d, storing key-value", partitionID)
	// Store in partition storage
	storage := partitionManager.GetPartition(partitionID).GetStorage()
	walEntry, err := storage.Set(kv.Key, kv.Value)
	if err != nil {
		log.Printf("DB-Node: Error storing key-value: %v", err)
		response := KeyValueResponse{
			Success: false,
			Error:   fmt.Sprintf("Error storing key-value for partition %d: %v", partitionID, err),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}
	log.Printf("DB-Node: Successfully stored key-value pair")

	// Asynchronously replicate to other nodes if this node is the leader
	p := partitionManager.GetPartition(partitionID)
	if p.Role == partition.RoleLeader {
		log.Printf("DB-Node: Starting replication for partition %d", partitionID)
		for _, replicaAddress := range p.Replicas {
			if replicaAddress != selfAddress && replicaAddress != "" {
				log.Printf("DB-Node: Replicating to %s", replicaAddress)
				go replicateWALEntriesToNode(replicaAddress, partitionID, []lsm.LogEntry{walEntry})
			}
		}
	}

	// Send success response
	response := KeyValueResponse{
		Success: true,
		Value:   kv.Value,
		Message: "Key-value pair set successfully",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
	log.Printf("DB-Node: Successfully completed SET operation")
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response := KeyValueResponse{
			Success: false,
			Error:   "Method not allowed",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Error reading request body",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}
	defer r.Body.Close()

	// Parse JSON request
	var kv struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(body, &kv); err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Invalid JSON format",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Validate input
	if kv.Key == "" {
		response := KeyValueResponse{
			Success: false,
			Error:   "Key is required",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Get partition ID from header
	partitionIDStr := r.Header.Get("X-Partition-ID")
	if partitionIDStr == "" {
		response := KeyValueResponse{
			Success: false,
			Error:   "Partition ID is required",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Invalid partition ID",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Check if this node is responsible for the partition
	if !partitionManager.HasPartition(partitionID) {
		response := KeyValueResponse{
			Success: false,
			Error:   "Partition not found",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Get value from partition storage
	storage := partitionManager.GetPartition(partitionID).GetStorage()
	value, exists := storage.Get(kv.Key)
	if !exists {
		response := KeyValueResponse{
			Success: false,
			Error:   "Key not found",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Send response
	response := KeyValueResponse{
		Success: true,
		Value:   value,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		response := KeyValueResponse{
			Success: false,
			Error:   "Method not allowed",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Error reading request body",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}
	defer r.Body.Close()

	// Parse JSON request
	var kv struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(body, &kv); err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Invalid JSON format",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Validate input
	if kv.Key == "" {
		response := KeyValueResponse{
			Success: false,
			Error:   "Key is required",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Get partition ID from header
	partitionIDStr := r.Header.Get("X-Partition-ID")
	if partitionIDStr == "" {
		response := KeyValueResponse{
			Success: false,
			Error:   "Partition ID is required",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   "Invalid partition ID",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Check if this node is responsible for the partition
	if !partitionManager.HasPartition(partitionID) {
		response := KeyValueResponse{
			Success: false,
			Error:   "Partition not found",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Check if this node is the leader for the partition
	if !partitionManager.IsLeader(partitionID) {
		// Forward to leader
		p := partitionManager.GetPartition(partitionID)
		if p == nil {
			response := KeyValueResponse{
				Success: false,
				Error:   "Partition not found",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(response)
			return
		}

		// Forward request to leader
		leaderURL := p.Leader + r.URL.Path
		resp, err := http.Post(leaderURL, "application/json", bytes.NewBuffer(body))
		if err != nil {
			response := KeyValueResponse{
				Success: false,
				Error:   "Error forwarding to leader",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(response)
			return
		}
		defer resp.Body.Close()

		// Copy response from leader
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	// Delete from partition storage
	storage := partitionManager.GetPartition(partitionID).GetStorage()
	walEntry, err := storage.Delete(kv.Key)
	if err != nil {
		response := KeyValueResponse{
			Success: false,
			Error:   fmt.Sprintf("Error deleting key for partition %d: %v", partitionID, err),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Asynchronously replicate to other nodes if this node is the leader
	p := partitionManager.GetPartition(partitionID)
	if p.Role == partition.RoleLeader {
		log.Printf("Node is leader for partition %d. Replicating delete for key '%s'.", partitionID, kv.Key)
		for _, replicaAddress := range p.Replicas {
			if replicaAddress != selfAddress && replicaAddress != "" {
				go replicateWALEntriesToNode(replicaAddress, partitionID, []lsm.LogEntry{walEntry})
			}
		}
	}

	// Send success response
	response := KeyValueResponse{
		Success: true,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// Handle partition assignment from controller
func handlePartitionAssignment(w http.ResponseWriter, r *http.Request) {
	log.Printf("DB-Node: Received partition assignment request from %s", r.RemoteAddr)

	if r.Method != http.MethodPost {
		log.Printf("DB-Node: Invalid method %s for partition assignment", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var info PartitionInfo
	if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
		log.Printf("DB-Node: Error decoding partition assignment: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	log.Printf("DB-Node: Received partition assignment - ID: %d, Role: %s, Leader: %s, Replicas: %v",
		info.ID, info.Role, info.Leader, info.Replicas)

	// Add or update partition
	partitionManager.AddPartition(
		info.ID,
		partition.Role(info.Role),
		info.Leader,
		info.Replicas,
	)
	log.Printf("DB-Node: Successfully processed partition assignment for partition %d", info.ID)

	w.WriteHeader(http.StatusOK)
}

func handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed for /replicate", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading replication request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload ReplicationPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, "Invalid JSON for replication payload", http.StatusBadRequest)
		return
	}

	if len(payload.WALEntries) == 0 {
		http.Error(w, "No WAL entries provided in replication payload", http.StatusBadRequest)
		return
	}

	log.Printf("Received replication request for partition %d with %d WAL entries", payload.PartitionID, len(payload.WALEntries))

	// Check if this node is responsible for the partition (it should be a replica or leader)
	// The partitionManager should have been updated by the controller via /partition/assign
	p := partitionManager.GetPartition(int(payload.PartitionID)) // Cast to int if your GetPartition expects int
	if p == nil {
		log.Printf("Replication error: Node does not have partition %d assigned", payload.PartitionID)
		http.Error(w, fmt.Sprintf("Node does not have partition %d assigned", payload.PartitionID), http.StatusNotFound)
		return
	}

	// It must be a replica (or leader itself, though typically leaders don't replicate to themselves this way)
	// For now, we assume if it has the partition, it can apply.
	// More robust check: if p.Role == partition.RoleReplica (or if it's the leader applying its own replicated entries in some test scenario)

	storage := p.GetStorage()
	if storage == nil {
		log.Printf("Replication error: Storage not found for partition %d", payload.PartitionID)
		http.Error(w, fmt.Sprintf("Storage not found for partition %d", payload.PartitionID), http.StatusInternalServerError)
		return
	}

	if err := storage.ApplyWALEntries(payload.WALEntries); err != nil {
		log.Printf("Replication error: Failed to apply WAL entries for partition %d: %v", payload.PartitionID, err)
		http.Error(w, fmt.Sprintf("Failed to apply WAL entries for partition %d: %v", payload.PartitionID, err), http.StatusInternalServerError)
		return
	}

	log.Printf("Successfully applied %d WAL entries for partition %d via replication", len(payload.WALEntries), payload.PartitionID)
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Replication successful for partition %d", payload.PartitionID)
}

func replicateWALEntriesToNode(nodeAddress string, pID int, entries []lsm.LogEntry) {
	if nodeAddress == "" || len(entries) == 0 {
		return // Nothing to do or nowhere to send
	}

	payload := ReplicationPayload{
		PartitionID: int64(pID),
		WALEntries:  entries,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Failed to marshal replication payload for node %s, partition %d: %v", nodeAddress, pID, err)
		return
	}

	// Ensure address starts with http:// or https://
	// This is a simplified check; a robust solution would parse and reconstruct the URL.
	destAddr := nodeAddress
	if !strings.HasPrefix(destAddr, "http://") && !strings.HasPrefix(destAddr, "https://") {
		destAddr = "http://" + destAddr
	}
	url := fmt.Sprintf("%s/replicate", destAddr)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Failed to create replication request to %s for partition %d: %v", url, pID, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second} // Increased timeout for replication calls
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Failed to send replication request to %s for partition %d: %v", url, pID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		// TODO: Implement more robust error handling for failed replications, e.g., retries, marking replica as stale.
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("Replication to node %s for partition %d failed with status %d: %s", nodeAddress, pID, resp.StatusCode, string(bodyBytes))
		return
	}

	log.Printf("Successfully sent replication request to node %s for partition %d (%d entries)", nodeAddress, pID, len(entries))
}

func main() {
	// Load configuration
	cfg, err := config.LoadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	appConfig = cfg
	selfAddress = fmt.Sprintf("%s:%d", appConfig.Host, appConfig.Port)
	log.Printf("Node self address configured as: %s", selfAddress)

	// Set up HTTP handlers
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/delete", handleDelete)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/partition/assign", handlePartitionAssignment)
	http.HandleFunc("/replicate", handleReplicate)

	log.Printf("DB Node running on %s", selfAddress)
	if err := http.ListenAndServe(selfAddress, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
