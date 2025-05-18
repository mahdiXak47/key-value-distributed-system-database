package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/mahdiXak47/key-value-distributed-system-database/db-node/partition"
)

var (
	partitionManager = partition.NewManager()
	nodeAddress      = "http://localhost:9001" // Should be configurable
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
}

type PartitionInfo struct {
	ID       int      `json:"id"`
	Role     string   `json:"role"`
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	// Get metrics for all partitions
	var totalMemTableSize int
	var maxLevels int
	partitions := make([]int, 0)

	for _, p := range partitionManager.GetPartitions() {
		storage := p.GetStorage()
		memTableSize, levels := storage.GetMetrics()
		totalMemTableSize += memTableSize
		if levels > maxLevels {
			maxLevels = levels
		}
		partitions = append(partitions, p.ID)
	}

	status := HealthStatus{
		Status:       "OK",
		MemTableSize: totalMemTableSize,
		Levels:       maxLevels,
		Partitions:   partitions,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(status)
}

func handleSet(w http.ResponseWriter, r *http.Request) {
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
		Key   string `json:"key"`
		Value string `json:"value"`
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

	// Store in partition storage
	storage := partitionManager.GetPartition(partitionID).GetStorage()
	storage.Set(kv.Key, kv.Value)

	// Send success response
	response := KeyValueResponse{
		Success: true,
		Value:   kv.Value,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
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
	storage.Delete(kv.Key)

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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var info PartitionInfo
	if err := json.NewDecoder(r.Body).Decode(&info); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Add or update partition
	partitionManager.AddPartition(
		info.ID,
		partition.Role(info.Role),
		info.Leader,
		info.Replicas,
	)

	w.WriteHeader(http.StatusOK)
}

func main() {
	// Set up HTTP handlers
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/delete", handleDelete)
	http.HandleFunc("/health", handleHealth)
	http.HandleFunc("/partition/assign", handlePartitionAssignment)

	log.Println("DB Node running on :9001")
	log.Fatal(http.ListenAndServe(":9001", nil))
}
