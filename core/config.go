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
	BasePath          string         `json:"base_path"`
	RolloverLimit     int64          `json:"rollover_limit"`
	Brokers           []BrokerConfig `json:"brokers"`
	ReplicationFactor int32          `json:"replication_factor"`
}

// BrokerConfig contains information on the other brokers/servers in the cluster
type BrokerConfig struct {
	Index int32  `json:"index"`
	Addr  string `json:"addr"`
}

type Option func(*Config)

func WithBasePath(path string) Option {
	return func(c *Config) { c.BasePath = path }
}

func WithReplicationFactor(factor int32) Option {
	return func(c *Config) { c.ReplicationFactor = factor }
}

func DefaultConfig() *Config {
	return &Config{
		BasePath:      "./data",
		RolloverLimit: 1024 * 1024, // 1MB
		Brokers: []BrokerConfig{
			{Index: 0, Addr: ":8080"},
			{Index: 1, Addr: ":8081"},
		},
		ReplicationFactor: 1,
	}
}

// LoadConfig tries to load from config.yml, if not found, use default config
func LoadConfig(overrides ...Option) *Config {
	var cfg Config
	b, err := os.ReadFile(ConfigPath)
	if err != nil {
		generateDefaultConfigFile()
		cfg = *DefaultConfig()
	}

	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Warn("Failed to parse config file, using default config:", err)
		cfg = *DefaultConfig()
	}

	// Apply overrides
	for _, opt := range overrides {
		opt(&cfg)
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
		return
	}
	log.Infof("Generated default config file at %s", ConfigPath)
}
