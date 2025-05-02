package broker

import (
	"context"
	"fmt"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/snac21/mqtt/handlers"
	"github.com/snac21/mqtt/logger"
	"github.com/snac21/mqtt/storage"
)

// Broker represents the MQTT broker instance
type Broker struct {
	server  *mqtt.Server
	config  *Config
	logger  *logger.Logger
	storage *storage.InfluxDBStorage
	mu      sync.RWMutex
}

// Config holds the broker configuration
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
func New(config *Config) (*Broker, error) {
	// Initialize logger
	logger := logger.New()

	// Initialize InfluxDB storage
	storage, err := storage.NewInfluxDBStorage(
		config.InfluxURL,
		config.InfluxToken,
		config.InfluxOrg,
		config.InfluxBucket,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize InfluxDB storage: %v", err)
	}

	return &Broker{
		config:  config,
		logger:  logger,
		storage: storage,
	}, nil
}

// Start initializes and runs the MQTT broker
func (b *Broker) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create new MQTT Server
	b.server = mqtt.New(&mqtt.Options{
		InlineClient: true,
	})

	// Add authentication hook if enabled
	if b.config.Auth {
		b.server.AddHook(new(auth.Hook), &auth.Options{
			Ledger: &auth.Ledger{
				Auth: auth.AuthRules{
					{Username: auth.RString(b.config.Username), Password: auth.RString(b.config.Password), Allow: true},
				},
			},
		})
	}

	// Initialize and register message handlers
	eventHook := handlers.NewEventHook()

	// Register control handler
	controlHandler := handlers.NewControlHandler(b.server)
	eventHook.RegisterHandler(controlHandler)

	// Register status handler
	statusHandler := handlers.NewStatusHandler(b.server)
	eventHook.RegisterHandler(statusHandler)

	// Add event hook
	b.server.AddHook(eventHook, nil)

	// Add InfluxDB storage hook
	b.server.AddHook(b.storage, nil)

	// Create TCP listener
	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: fmt.Sprintf(":%d", b.config.Port),
	})
	err := b.server.AddListener(tcp)
	if err != nil {
		return fmt.Errorf("failed to add TCP listener: %v", err)
	}

	// Start the broker
	err = b.server.Serve()
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}

	b.logger.Info("MQTT broker started", "port", b.config.Port)
	return nil
}

// Stop gracefully shuts down the MQTT broker
func (b *Broker) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.server == nil {
		return nil
	}

	// Close InfluxDB storage
	b.storage.Close()

	// Close MQTT server
	err := b.server.Close()
	if err != nil {
		return fmt.Errorf("failed to stop server: %v", err)
	}

	b.logger.Info("MQTT broker stopped")
	return nil
}

// GetServer returns the MQTT server instance
func (b *Broker) GetServer() *mqtt.Server {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.server
}
