package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/snac21/mqtt/internal/config"
	"github.com/snac21/mqtt/internal/discovery"
	"github.com/snac21/mqtt/internal/handlers"
	"github.com/snac21/mqtt/internal/hooks"
	"github.com/snac21/mqtt/internal/logger"
)

// Broker represents the MQTT broker
type Broker struct {
	server   *mqtt.Server
	config   *config.Config
	storage  *hooks.InfluxDBHook
	handlers map[string]handlers.MessageHandler
	registry discovery.Registry
	mu       sync.RWMutex
	logger   *logger.Logger
}

// New creates a new MQTT broker instance
func New(cfg *config.Config, logger *logger.Logger) (*Broker, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Create new MQTT server with default options
	server := mqtt.New(&mqtt.Options{
		InlineClient: true,
	})

	// Create broker instance
	broker := &Broker{
		server:   server,
		config:   cfg,
		handlers: make(map[string]handlers.MessageHandler),
		logger:   logger,
	}

	// Initialize components
	if err := broker.initializeRegistry(); err != nil {
		return nil, fmt.Errorf("failed to initialize registry: %w", err)
	}

	if err := broker.initializeStorage(); err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	if err := broker.initializeAuth(); err != nil {
		return nil, fmt.Errorf("failed to initialize auth: %w", err)
	}

	if err := broker.initializeHandlers(); err != nil {
		return nil, fmt.Errorf("failed to initialize handlers: %w", err)
	}

	return broker, nil
}

// initializeRegistry initializes the service registry
func (b *Broker) initializeRegistry() error {
	if b.config.Discovery.Address == "" {
		return nil
	}

	var registry discovery.Registry
	var err error

	// Create service instance
	instance := &discovery.ServiceInstance{
		ID:   fmt.Sprintf("%d", b.config.Broker.Port),
		Name: "mqtt-broker",
		Host: b.config.Discovery.Address,
		Port: int(b.config.Discovery.Port),
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	// Initialize registry based on configuration
	switch b.config.Discovery.Type {
	case "nacos":
		nacosConfig := &config.NacosConfig{
			ServerAddr: b.config.Discovery.Address,
			Namespace:  b.config.Discovery.Namespace,
			Group:      b.config.Discovery.Group,
			Username:   b.config.Discovery.Username,
			Password:   b.config.Discovery.Password,
		}
		registry, err = discovery.NewNacosRegistry(nacosConfig)
	case "consul":
		consulConfig := &config.ConsulConfig{
			Address: b.config.Discovery.Address,
			Token:   b.config.Discovery.Token,
			Scheme:  b.config.Discovery.Scheme,
		}
		registry, err = discovery.NewConsulRegistry(consulConfig)
	default:
		return fmt.Errorf("unsupported registry type: %s", b.config.Discovery.Type)
	}

	if err != nil {
		return fmt.Errorf("failed to initialize registry: %w", err)
	}

	// Register service with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := registry.Register(ctx, instance); err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}

	b.registry = registry
	return nil
}

// initializeStorage initializes the storage hook
func (b *Broker) initializeStorage() error {
	if b.config.Storage.URL == "" {
		return nil
	}

	storage, err := hooks.NewInfluxDBHook(
		b.config.Storage.URL,
		b.config.Storage.Token,
		b.config.Storage.Organization,
		b.config.Storage.Bucket,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %w", err)
	}

	b.storage = storage
	return b.server.AddHook(storage, nil)
}

// initializeAuth initializes the authentication hook
func (b *Broker) initializeAuth() error {
	if !b.config.Broker.Auth.Enabled {
		return nil
	}

	authHook := &auth.AllowHook{}
	return b.server.AddHook(authHook, nil)
}

// Start starts the MQTT broker
func (b *Broker) Start(ctx context.Context) error {
	// Create TCP listener
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: fmt.Sprintf(":%d", b.config.Broker.Port),
	})
	if err := b.server.AddListener(tcp); err != nil {
		return fmt.Errorf("failed to add TCP listener: %w", err)
	}

	// Register with service registry if configured
	if b.config.Discovery.Address != "" {
		instance := &discovery.ServiceInstance{
			ID:   fmt.Sprintf("mqtt-broker-%s", uuid.New().String()),
			Name: "mqtt-broker",
			Host: "localhost", // TODO: Make configurable
			Port: b.config.Broker.Port,
			Metadata: map[string]string{
				"version": "1.0.0",
			},
		}

		if err := b.registry.Register(ctx, instance); err != nil {
			return fmt.Errorf("failed to register with service registry: %w", err)
		}

		// Deregister on shutdown
		go func() {
			<-ctx.Done()
			if err := b.registry.Deregister(ctx, instance.ID); err != nil {
				b.logger.Error("Failed to deregister from service registry", err)
			}
		}()
	}

	// Start server
	if err := b.server.Serve(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	b.logger.Info("MQTT broker started", map[string]interface{}{
		"port": b.config.Broker.Port,
	})

	return nil
}

// Stop stops the MQTT broker
func (b *Broker) Stop() {
	b.server.Close()
	if b.storage != nil {
		b.storage.Close()
	}
	b.logger.Info("MQTT broker stopped")
}

// RegisterHandler registers a message handler
func (b *Broker) RegisterHandler(handler handlers.MessageHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[handler.Type()] = handler
}

// initializeHandlers initializes message handlers
func (b *Broker) initializeHandlers() error {
	// 创建路由处理器
	router := handlers.NewRouter()

	// 注册所有处理器到路由
	for _, handler := range b.handlers {
		router.RegisterHandler(handler)
	}

	// 创建事件钩子
	eventHook, err := hooks.NewEventHook()
	if err != nil {
		return fmt.Errorf("failed to create event hook: %w", err)
	}

	// 设置路由处理器
	eventHook.SetRouter(router)

	// 添加事件钩子到服务器
	if err := b.server.AddHook(eventHook, nil); err != nil {
		return fmt.Errorf("failed to add event hook: %w", err)
	}

	return nil
}

// mqttServer implements the handlers.Server interface
type mqttServer struct {
	server *mqtt.Server
}

// Publish publishes a message to a topic
func (s *mqttServer) Publish(clientID, topic string, payload []byte, qos byte, retain bool) {
	s.server.Publish(topic, payload, retain, qos)
}

// GetMetrics returns broker metrics
func (b *Broker) GetMetrics() map[string]interface{} {
	clients := b.server.Clients.GetAll()
	subscriptions := make(map[string]struct{})
	for _, client := range clients {
		for filter := range client.State.Subscriptions.GetAll() {
			subscriptions[filter] = struct{}{}
		}
	}
	return map[string]interface{}{
		"clients": len(clients),
		"topics":  len(subscriptions),
	}
}

// GetClients returns connected clients
func (b *Broker) GetClients() []*handlers.Client {
	clients := make([]*handlers.Client, 0)
	for _, client := range b.server.Clients.GetAll() {
		clients = append(clients, &handlers.Client{
			ID:       client.ID,
			Username: string(client.Properties.Username),
			Clean:    client.Properties.Clean,
		})
	}
	return clients
}

// GetTopics returns active topics
func (b *Broker) GetTopics() []string {
	subscriptions := make(map[string]struct{})
	for _, client := range b.server.Clients.GetAll() {
		for filter := range client.State.Subscriptions.GetAll() {
			subscriptions[filter] = struct{}{}
		}
	}
	topics := make([]string, 0, len(subscriptions))
	for topic := range subscriptions {
		topics = append(topics, topic)
	}
	return topics
}

// AddListener adds a listener to the server
func (b *Broker) AddListener(listener interface{}) error {
	if l, ok := listener.(listeners.Listener); ok {
		return b.server.AddListener(l)
	}
	return fmt.Errorf("invalid listener type")
}

// Serve starts the server
func (b *Broker) Serve() error {
	return b.server.Serve()
}

// Close closes the server
func (b *Broker) Close() {
	b.server.Close()
}

// AddHook adds a hook to the server
func (b *Broker) AddHook(hook interface{}, config interface{}) error {
	if h, ok := hook.(mqtt.Hook); ok {
		return b.server.AddHook(h, config)
	}
	return fmt.Errorf("invalid hook type")
}
