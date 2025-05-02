package broker

import (
	"context"
	"fmt"
	"sync"

	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/snac21/mqtt/internal/handlers"
	"github.com/snac21/mqtt/internal/hooks"
	"github.com/snac21/mqtt/internal/logger"
)

// Broker represents the MQTT broker
type Broker struct {
	server   *mqttserver.Server
	config   *Config
	storage  *hooks.InfluxDBHook
	handlers map[string]handlers.MessageHandler
	mu       sync.RWMutex
	logger   *logger.Logger
}

// Config represents broker configuration
type Config struct {
	Port         int
	Auth         bool
	Username     string
	Password     string
	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string
}

// New creates a new MQTT broker instance
func New(config *Config, logger *logger.Logger) (*Broker, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	// Create new MQTT server with default options
	server := mqttserver.New(&mqttserver.Options{
		InlineClient: true,
	})

	// Create broker instance
	broker := &Broker{
		server:   server,
		config:   config,
		handlers: make(map[string]handlers.MessageHandler),
		logger:   logger,
	}

	// Initialize storage
	if config.InfluxURL != "" {
		storage, err := hooks.NewInfluxDBHook(config.InfluxURL, config.InfluxToken, config.InfluxOrg, config.InfluxBucket)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize storage: %w", err)
		}
		broker.storage = storage
		if err := server.AddHook(storage, nil); err != nil {
			return nil, fmt.Errorf("failed to add storage hook: %w", err)
		}
	}

	// Configure authentication if enabled
	if config.Auth {
		authHook := &auth.AllowHook{}
		if err := server.AddHook(authHook, nil); err != nil {
			return nil, fmt.Errorf("failed to add auth hook: %w", err)
		}
	}

	// Initialize message handlers
	if err := broker.initializeHandlers(); err != nil {
		return nil, fmt.Errorf("failed to initialize handlers: %w", err)
	}

	return broker, nil
}

// Start starts the MQTT broker
func (b *Broker) Start(ctx context.Context) error {
	// Create TCP listener
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: fmt.Sprintf(":%d", b.config.Port),
	})
	if err := b.server.AddListener(tcp); err != nil {
		return fmt.Errorf("failed to add TCP listener: %w", err)
	}

	// Start server
	if err := b.server.Serve(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	b.logger.Info("MQTT broker started", map[string]interface{}{
		"port": b.config.Port,
	})

	// Wait for context cancellation
	go func() {
		<-ctx.Done()
		b.Stop()
	}()

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

// initializeHandlers initializes default message handlers
func (b *Broker) initializeHandlers() error {
	handlers := []handlers.MessageHandler{
		handlers.NewAuthHandler(b),
		handlers.NewControlHandler(b),
		handlers.NewDataHandler(b),
	}

	for _, handler := range handlers {
		b.RegisterHandler(handler)
	}

	return nil
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

// Publish publishes a message to a topic
func (b *Broker) Publish(clientID, topic string, payload []byte, qos byte, retain bool) {
	b.server.Publish(topic, payload, retain, qos)
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
	if h, ok := hook.(mqttserver.Hook); ok {
		return b.server.AddHook(h, config)
	}
	return fmt.Errorf("invalid hook type")
}
