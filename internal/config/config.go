package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	Broker    BrokerConfig    `yaml:"broker"`
	Web       WebConfig       `yaml:"web"`
	Discovery DiscoveryConfig `yaml:"discovery"`
	Storage   StorageConfig   `yaml:"storage"`
	Logger    LoggerConfig    `yaml:"logger"`
}

// BrokerConfig represents MQTT broker configuration
type BrokerConfig struct {
	Port int        `yaml:"port"`
	Auth AuthConfig `yaml:"auth"`
}

// AuthConfig represents authentication configuration
type AuthConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// WebConfig represents web server configuration
type WebConfig struct {
	Port string `yaml:"port"`
}

// DiscoveryConfig represents service discovery configuration
type DiscoveryConfig struct {
	Type      string `yaml:"type"`
	Address   string `yaml:"address"`
	Port      uint64 `yaml:"port"`
	Namespace string `yaml:"namespace"`
	Group     string `yaml:"group"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Token     string `yaml:"token"`
	Scheme    string `yaml:"scheme"`
}

// StorageConfig represents storage configuration
type StorageConfig struct {
	URL          string `yaml:"url"`
	Token        string `yaml:"token"`
	Organization string `yaml:"organization"`
	Bucket       string `yaml:"bucket"`
}

// LoggerConfig represents logger configuration
type LoggerConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	Output string `yaml:"output"`
}

// NacosConfig represents the configuration for Nacos
type NacosConfig struct {
	ServerAddr string
	Namespace  string
	Group      string
	Username   string
	Password   string
}

// ConsulConfig represents the configuration for Consul
type ConsulConfig struct {
	Address string
	Token   string
	Scheme  string
}

// ServiceInstance represents a service instance in the registry
type ServiceInstance struct {
	ID       string
	Name     string
	Host     string
	Port     int
	Metadata map[string]string
}

// Load loads configuration from config.yaml
func Load() (*Config, error) {
	data, err := os.ReadFile("config/config.yaml")
	if err != nil {
		return nil, err
	}

	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// Override with environment variables if set
	if port := os.Getenv("MQTT_PORT"); port != "" {
		// Parse port and set to cfg.Broker.Port
	}
	// Add more environment variable overrides as needed

	return cfg, nil
}
