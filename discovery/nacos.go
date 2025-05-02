package discovery

import (
	"fmt"

	"gitee.com/snac21/mqtt/logger"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/vo"
)

// NacosDiscovery implements service discovery using Nacos
type NacosDiscovery struct {
	client naming_client.INamingClient
	logger *logger.Logger
}

// NewNacosDiscovery creates a new Nacos discovery instance
func NewNacosDiscovery(serverAddr string, port uint64) (*NacosDiscovery, error) {
	sc := []constant.ServerConfig{
		{
			IpAddr: serverAddr,
			Port:   port,
		},
	}

	cc := constant.ClientConfig{
		TimeoutMs:           5000,
		NotLoadCacheAtStart: true,
		LogDir:              "/tmp/nacos/log",
		CacheDir:            "/tmp/nacos/cache",
		LogLevel:            "debug",
	}

	client, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: sc,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Nacos client: %v", err)
	}

	return &NacosDiscovery{
		client: client,
		logger: logger.New(),
	}, nil
}

// RegisterService registers the MQTT broker service with Nacos
func (d *NacosDiscovery) RegisterService(serviceName, ip string, port uint64) error {
	success, err := d.client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: serviceName,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"mqtt": "broker"},
	})
	if err != nil {
		return fmt.Errorf("failed to register service: %v", err)
	}
	if !success {
		return fmt.Errorf("failed to register service: registration unsuccessful")
	}

	d.logger.Info("Service registered with Nacos", "service", serviceName, "ip", ip, "port", port)
	return nil
}

// DeregisterService removes the service registration
func (d *NacosDiscovery) DeregisterService(serviceName, ip string, port uint64) error {
	success, err := d.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: serviceName,
		Ephemeral:   true,
	})
	if err != nil {
		return fmt.Errorf("failed to deregister service: %v", err)
	}
	if !success {
		return fmt.Errorf("failed to deregister service: deregistration unsuccessful")
	}

	d.logger.Info("Service deregistered from Nacos", "service", serviceName)
	return nil
}

// GetServiceInstances retrieves all instances of a service
func (d *NacosDiscovery) GetServiceInstances(serviceName string) ([]string, error) {
	instances, err := d.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		HealthyOnly: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get service instances: %v", err)
	}

	addresses := make([]string, 0, len(instances))
	for _, instance := range instances {
		addresses = append(addresses, fmt.Sprintf("%s:%d", instance.Ip, instance.Port))
	}

	return addresses, nil
}
