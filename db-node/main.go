package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

var dbStorage = NewStorage()

type HealthStatus struct {
	Status       string `json:"status"`
	MemTableSize int    `json:"memTableSize"`
	Levels       int    `json:"levels"`
}

type KeyValueResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	metrics := dbStorage.GetMetrics()
	status := HealthStatus{
		Status:       "OK",
		MemTableSize: metrics["memTableSize"].(int),
		Levels:       metrics["levels"].(int),
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

	// Store in Storage
	dbStorage.Set(kv.Key, kv.Value)

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

	// Get value from storage
	value, exists := dbStorage.Get(kv.Key)
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

	// Delete from storage
	dbStorage.Delete(kv.Key)

	// Send success response
	response := KeyValueResponse{
		Success: true,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func main() {
	// Set up HTTP handlers
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/delete", handleDelete)
	http.HandleFunc("/health", handleHealth)

	log.Println("DB Node running on :9001")
	log.Fatal(http.ListenAndServe(":9001", nil))
}
