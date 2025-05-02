package config

import (
	"os"
	"strconv"
)

// Config holds all configuration settings
type Config struct {
	Broker    BrokerConfig
	Web       WebConfig
	Discovery DiscoveryConfig
	Storage   StorageConfig
}

// BrokerConfig holds MQTT broker settings
type BrokerConfig struct {
	Port     int
	Auth     bool
	Username string
	Password string
}

// WebConfig holds web server settings
type WebConfig struct {
	Port string
}

// DiscoveryConfig holds service discovery settings
type DiscoveryConfig struct {
	Provider string
	Nacos    NacosConfig
}

// NacosConfig holds Nacos specific settings
type NacosConfig struct {
	Address string
	Port    uint64
}

// StorageConfig holds storage settings
type StorageConfig struct {
	Provider string
	InfluxDB InfluxDBConfig
}

// InfluxDBConfig holds InfluxDB specific settings
type InfluxDBConfig struct {
	URL    string
	Token  string
	Org    string
	Bucket string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() *Config {
	return &Config{
		Broker: BrokerConfig{
			Port:     getEnvInt("MQTT_PORT", 1883),
			Auth:     getEnvBool("MQTT_AUTH", false),
			Username: getEnvString("MQTT_USERNAME", ""),
			Password: getEnvString("MQTT_PASSWORD", ""),
		},
		Web: WebConfig{
			Port: getEnvString("WEB_PORT", ":8080"),
		},
		Discovery: DiscoveryConfig{
			Provider: getEnvString("DISCOVERY_PROVIDER", "nacos"),
			Nacos: NacosConfig{
				Address: getEnvString("NACOS_ADDRESS", "localhost"),
				Port:    getEnvUint64("NACOS_PORT", 8848),
			},
		},
		Storage: StorageConfig{
			Provider: getEnvString("STORAGE_PROVIDER", "influxdb"),
			InfluxDB: InfluxDBConfig{
				URL:    getEnvString("INFLUXDB_URL", "http://localhost:8086"),
				Token:  getEnvString("INFLUXDB_TOKEN", ""),
				Org:    getEnvString("INFLUXDB_ORG", "mqtt"),
				Bucket: getEnvString("INFLUXDB_BUCKET", "mqtt"),
			},
		},
	}
}

// Helper functions for environment variable parsing
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvUint64(key string, defaultValue uint64) uint64 {
	if value := os.Getenv(key); value != "" {
		if uintValue, err := strconv.ParseUint(value, 10, 64); err == nil {
			return uintValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}
