package discovery

import (
	"context"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/snac21/mqtt/internal/config"
)

// NacosRegistry implements the Registry interface for Nacos
type NacosRegistry struct {
	client naming_client.INamingClient
	config *config.NacosConfig
}

// NewNacosRegistry creates a new Nacos registry instance
func NewNacosRegistry(config *config.NacosConfig) (*NacosRegistry, error) {
	if config == nil {
		return nil, fmt.Errorf("nacos config is required")
	}

	// Create server configs
	serverConfigs := []constant.ServerConfig{
		{
			IpAddr: config.ServerAddr,
			Port:   8848,
		},
	}

	// Create client config
	clientConfig := constant.ClientConfig{
		NamespaceId:         config.Namespace,
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              "/tmp/nacos/log",
		CacheDir:            "/tmp/nacos/cache",
		Username:            config.Username,
		Password:            config.Password,
	}

	// Create naming client
	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create naming client: %w", err)
	}

	return &NacosRegistry{
		client: client,
		config: config,
	}, nil
}

// Register registers a service instance with Nacos
func (r *NacosRegistry) Register(ctx context.Context, instance *config.ServiceInstance) error {
	param := vo.RegisterInstanceParam{
		Ip:          instance.Host,
		Port:        uint64(instance.Port),
		ServiceName: instance.Name,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    instance.Metadata,
		GroupName:   r.config.Group,
	}

	success, err := r.client.RegisterInstance(param)
	if err != nil {
		return fmt.Errorf("failed to register instance: %w", err)
	}
	if !success {
		return fmt.Errorf("failed to register instance: registration failed")
	}

	return nil
}

// Deregister deregisters a service instance from Nacos
func (r *NacosRegistry) Deregister(ctx context.Context, instanceID string) error {
	param := vo.DeregisterInstanceParam{
		Ip:          instanceID,
		ServiceName: r.config.Group,
		GroupName:   r.config.Group,
		Ephemeral:   true,
	}

	success, err := r.client.DeregisterInstance(param)
	if err != nil {
		return fmt.Errorf("failed to deregister instance: %w", err)
	}
	if !success {
		return fmt.Errorf("failed to deregister instance: deregistration failed")
	}

	return nil
}

// GetService returns all instances of a service from Nacos
func (r *NacosRegistry) GetService(ctx context.Context, serviceName string) ([]*config.ServiceInstance, error) {
	param := vo.GetServiceParam{
		ServiceName: serviceName,
		GroupName:   r.config.Group,
	}

	service, err := r.client.GetService(param)
	if err != nil {
		return nil, fmt.Errorf("failed to get service: %w", err)
	}

	instances := make([]*config.ServiceInstance, 0, len(service.Hosts))
	for _, host := range service.Hosts {
		instances = append(instances, &config.ServiceInstance{
			ID:       host.InstanceId,
			Name:     serviceName,
			Host:     host.Ip,
			Port:     int(host.Port),
			Metadata: host.Metadata,
		})
	}

	return instances, nil
}

// Watch watches for service changes in Nacos
func (r *NacosRegistry) Watch(ctx context.Context, serviceName string) (<-chan []*config.ServiceInstance, error) {
	ch := make(chan []*config.ServiceInstance, 10)

	param := &vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   r.config.Group,
		SubscribeCallback: func(services []model.Instance, err error) {
			if err != nil {
				return
			}

			instances := make([]*config.ServiceInstance, 0, len(services))
			for _, service := range services {
				instances = append(instances, &config.ServiceInstance{
					ID:       service.InstanceId,
					Name:     serviceName,
					Host:     service.Ip,
					Port:     int(service.Port),
					Metadata: service.Metadata,
				})
			}

			select {
			case ch <- instances:
			default:
				// Channel is full, skip this update
			}
		},
	}

	err := r.client.Subscribe(param)
	if err != nil {
		close(ch)
		return nil, fmt.Errorf("failed to subscribe to service: %w", err)
	}

	// Start a goroutine to handle context cancellation
	go func() {
		<-ctx.Done()
		r.client.Unsubscribe(param)
		close(ch)
	}()

	return ch, nil
}
