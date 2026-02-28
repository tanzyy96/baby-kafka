package core

import (
	"encoding/json"
	"os"

	"github.com/charmbracelet/log"
)

const (
	ConfigPath = "config.json"
)

type Config struct {
	BasePath      string `json:"base_path"`
	RolloverLimit int64  `json:"rollover_limit"`
	ServerPort    string `json:"server_port"`
}

func DefaultConfig() *Config {
	return &Config{
		BasePath:      "./data",
		RolloverLimit: 1024 * 1024, // 1MB
		ServerPort:    ":8080",
	}
}

// Try to load from config.yml, if not found, use default config
func LoadConfig() *Config {
	b, err := os.ReadFile(ConfigPath)
	if err != nil {
		generateDefaultConfigFile()
		return DefaultConfig()
	}

	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Warn("Failed to parse config file, using default config:", err)
		return DefaultConfig()
	}

	return &cfg
}

func generateDefaultConfigFile() {
	cfg := DefaultConfig()
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Warn("Failed to generate default config file:", err)
		return
	}

	if err := os.WriteFile(ConfigPath, b, 0o644); err != nil {
		log.Warn("Failed to write default config file:", err)
	}
	log.Infof("Generated default config file at %s", ConfigPath)
}
