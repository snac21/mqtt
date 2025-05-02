package discovery

import (
	"context"
	"fmt"

	"github.com/hashicorp/consul/api"
	"github.com/snac21/mqtt/internal/config"
)

// ConsulRegistry implements the Registry interface for Consul
type ConsulRegistry struct {
	client *api.Client
	config *config.ConsulConfig
}

// NewConsulRegistry creates a new Consul registry instance
func NewConsulRegistry(config *config.ConsulConfig) (*ConsulRegistry, error) {
	if config == nil {
		return nil, fmt.Errorf("consul config is required")
	}

	// Create consul client config
	clientConfig := api.DefaultConfig()
	clientConfig.Address = config.Address
	clientConfig.Token = config.Token

	// Create consul client
	client, err := api.NewClient(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create consul client: %w", err)
	}

	return &ConsulRegistry{
		client: client,
		config: config,
	}, nil
}

// Register registers a service instance with Consul
func (r *ConsulRegistry) Register(ctx context.Context, instance *ServiceInstance) error {
	registration := &api.AgentServiceRegistration{
		ID:      instance.ID,
		Name:    instance.Name,
		Address: instance.Host,
		Port:    instance.Port,
		Meta:    instance.Metadata,
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://%s:%d/health", instance.Host, instance.Port),
			Interval: "10s",
			Timeout:  "5s",
		},
	}

	err := r.client.Agent().ServiceRegister(registration)
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	return nil
}

// Deregister deregisters a service instance from Consul
func (r *ConsulRegistry) Deregister(ctx context.Context, instanceID string) error {
	err := r.client.Agent().ServiceDeregister(instanceID)
	if err != nil {
		return fmt.Errorf("failed to deregister service: %w", err)
	}

	return nil
}

// GetService returns all instances of a service from Consul
func (r *ConsulRegistry) GetService(ctx context.Context, serviceName string) ([]*ServiceInstance, error) {
	services, _, err := r.client.Health().Service(serviceName, "", true, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get service: %w", err)
	}

	instances := make([]*ServiceInstance, 0, len(services))
	for _, service := range services {
		instances = append(instances, &ServiceInstance{
			ID:       service.Service.ID,
			Name:     service.Service.Service,
			Host:     service.Service.Address,
			Port:     service.Service.Port,
			Metadata: service.Service.Meta,
		})
	}

	return instances, nil
}

// Watch watches for service changes
func (r *ConsulRegistry) Watch(ctx context.Context, serviceName string) (<-chan []*ServiceInstance, error) {
	ch := make(chan []*ServiceInstance, 10)

	// Start a goroutine to watch for changes
	go func() {
		lastIndex := uint64(0)
		for {
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:
				services, meta, err := r.client.Health().Service(serviceName, "", true, &api.QueryOptions{
					WaitIndex: lastIndex,
				})
				if err != nil {
					continue
				}

				if meta.LastIndex > lastIndex {
					lastIndex = meta.LastIndex
					instances := make([]*ServiceInstance, 0, len(services))
					for _, service := range services {
						instances = append(instances, &ServiceInstance{
							ID:       service.Service.ID,
							Name:     service.Service.Service,
							Host:     service.Service.Address,
							Port:     service.Service.Port,
							Metadata: service.Service.Meta,
						})
					}

					select {
					case ch <- instances:
					default:
						// Channel is full, skip this update
					}
				}
			}
		}
	}()

	return ch, nil
}
