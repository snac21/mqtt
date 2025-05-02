package discovery

import (
	"context"
)

// ServiceInstance represents a service instance in the registry
type ServiceInstance struct {
	ID       string
	Name     string
	Host     string
	Port     int
	Metadata map[string]string
}

// Registry defines the interface for service registration
type Registry interface {
	// Register registers a service instance
	Register(ctx context.Context, instance *ServiceInstance) error

	// Deregister deregisters a service instance
	Deregister(ctx context.Context, instanceID string) error

	// GetService returns all instances of a service
	GetService(ctx context.Context, serviceName string) ([]*ServiceInstance, error)

	// Watch watches for service changes
	Watch(ctx context.Context, serviceName string) (<-chan []*ServiceInstance, error)
}

// Config represents the configuration for service registration
type Config struct {
	// Type is the type of registry (nacos, consul)
	Type string

	// NacosConfig is the configuration for Nacos
	NacosConfig *NacosConfig

	// ConsulConfig is the configuration for Consul
	ConsulConfig *ConsulConfig
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
