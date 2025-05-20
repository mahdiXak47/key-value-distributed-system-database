package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const (
	baseURL    = "http://localhost:8081" // Load balancer URL
	maxRetries = 3
	retryDelay = time.Second * 2
)

type KeyValueRequest struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type KeyValueResponse struct {
	Success bool   `json:"success"`
	Value   string `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

func makeRequest(method, endpoint string, data KeyValueRequest) (*KeyValueResponse, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	req, err := http.NewRequest(method, baseURL+endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	var response KeyValueResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("error unmarshaling response: %v", err)
	}

	return &response, nil
}

func main() {
	// Start the web server
	http.HandleFunc("/", handleHome)
	http.HandleFunc("/set", handleSet)
	http.HandleFunc("/get", handleGet)
	http.HandleFunc("/delete", handleDelete)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	log.Println("Starting client server on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func handleHome(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "templates/index.html")
}

func handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse JSON request
	var requestData struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		log.Printf("[Client] Error decoding request: %v", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	log.Printf("[Client] Received set request - Key: %s, Value: %s", requestData.Key, requestData.Value)

	// Send request to load balancer
	response, err := makeRequest("POST", "/set", KeyValueRequest{
		Key:   requestData.Key,
		Value: requestData.Value,
	})

	if err != nil {
		log.Printf("[Client] Error sending request: %v", err)
		response = &KeyValueResponse{
			Success: false,
			Error:   fmt.Sprintf("Error: %v", err),
		}
	}

	if response.Success {
		response.Value = fmt.Sprintf("Successfully set key '%s' with value '%s'", requestData.Key, requestData.Value)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse JSON request
	var requestData struct {
		Key string `json:"key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		log.Printf("[Client] Error decoding request: %v", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	log.Printf("[Client] Received get request - Key: %s", requestData.Key)

	response, err := makeRequest("GET", "/get", KeyValueRequest{Key: requestData.Key})
	if err != nil {
		log.Printf("[Client] Error sending request: %v", err)
		response = &KeyValueResponse{
			Success: false,
			Error:   fmt.Sprintf("Error: %v", err),
		}
	}

	if response.Success {
		response.Value = fmt.Sprintf("Value for key '%s': %s", requestData.Key, response.Value)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse JSON request
	var requestData struct {
		Key string `json:"key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&requestData); err != nil {
		log.Printf("[Client] Error decoding request: %v", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	log.Printf("[Client] Received delete request - Key: %s", requestData.Key)

	response, err := makeRequest("DELETE", "/delete", KeyValueRequest{Key: requestData.Key})
	if err != nil {
		log.Printf("[Client] Error sending request: %v", err)
		response = &KeyValueResponse{
			Success: false,
			Error:   fmt.Sprintf("Error: %v", err),
		}
	}

	if response.Success {
		response.Value = fmt.Sprintf("Successfully deleted key '%s'", requestData.Key)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
