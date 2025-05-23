package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Config represents the DB node configuration
type Config struct {
	NodeID            string `json:"nodeId"`
	Host              string `json:"host"`
	Port              int    `json:"port"`
	DataDir           string `json:"dataDir"`
	ControllerURL     string `json:"controllerUrl"`
	PartitionSettings struct {
		MemTableSizeThreshold int `json:"memTableSizeThreshold"`
		LevelSizeThreshold    int `json:"levelSizeThreshold"`
		MaxLevels             int `json:"maxLevels"`
	} `json:"partitionSettings"`
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		NodeID:        "node-1",
		Host:          "localhost",
		Port:          9001,
		DataDir:       "./data",
		ControllerURL: "http://localhost:8000",
		PartitionSettings: struct {
			MemTableSizeThreshold int `json:"memTableSizeThreshold"`
			LevelSizeThreshold    int `json:"levelSizeThreshold"`
			MaxLevels             int `json:"maxLevels"`
		}{
			MemTableSizeThreshold: 1000,
			LevelSizeThreshold:    5,
			MaxLevels:             7,
		},
	}
}

// LoadConfig loads configuration from file
func LoadConfig(path string) (*Config, error) {
	// Use default config if file doesn't exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return DefaultConfig(), nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	config := DefaultConfig()
	if err := json.NewDecoder(file).Decode(config); err != nil {
		return nil, err
	}

	return config, nil
}

// SaveConfig saves configuration to file
func (c *Config) SaveConfig(path string) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(c)
}
