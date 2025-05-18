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

func retryRequest(method, endpoint string, data KeyValueRequest) (*KeyValueResponse, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		response, err := makeRequest(method, endpoint, data)
		if err == nil && response.Success {
			return response, nil
		}
		lastErr = err
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	return &KeyValueResponse{
		Success: false,
		Error:   fmt.Sprintf("Failed after %d retries: %v", maxRetries, lastErr),
	}, nil
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
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.FormValue("key")
	value := r.FormValue("value")

	response, _ := retryRequest(http.MethodPost, "/set", KeyValueRequest{Key: key, Value: value})
	if response.Success {
		response.Value = fmt.Sprintf("Successfully set key '%s' with value '%s'", key, value)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.FormValue("key")

	response, _ := retryRequest(http.MethodGet, "/get", KeyValueRequest{Key: key})
	if response.Success {
		response.Value = fmt.Sprintf("Value for key '%s': %s", key, response.Value)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.FormValue("key")

	response, _ := retryRequest(http.MethodDelete, "/delete", KeyValueRequest{Key: key})
	if response.Success {
		response.Value = fmt.Sprintf("Successfully deleted key '%s'", key)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
