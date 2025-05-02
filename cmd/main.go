package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/snac21/mqtt/internal/broker"
	"github.com/snac21/mqtt/internal/config"
	"github.com/snac21/mqtt/internal/logger"
	"github.com/snac21/mqtt/internal/web"
)

func main() {
	// Parse command line flags
	port := flag.Int("port", 1883, "MQTT broker port")
	webPort := flag.String("web-port", ":8080", "Web management interface port")
	influxURL := flag.String("influx-url", "http://localhost:8086", "InfluxDB URL")
	influxToken := flag.String("influx-token", "", "InfluxDB token")
	influxOrg := flag.String("influx-org", "mqtt", "InfluxDB organization")
	influxBucket := flag.String("influx-bucket", "mqtt", "InfluxDB bucket")
	flag.Parse()

	// Initialize logger
	log := logger.New(&logger.Config{
		Level: "info",
	})

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create and start MQTT broker
	cfg := &config.Config{
		Broker: config.BrokerConfig{
			Port: *port,
			Auth: config.AuthConfig{
				Enabled:  false, // TODO: Make configurable
				Username: "",
				Password: "",
			},
		},
		Storage: config.StorageConfig{
			URL:          *influxURL,
			Token:        *influxToken,
			Organization: *influxOrg,
			Bucket:       *influxBucket,
		},
	}

	broker, err := broker.New(cfg, log)
	if err != nil {
		log.Error("Failed to create MQTT broker", err)
		os.Exit(1)
	}

	// Start MQTT broker
	if err := broker.Start(ctx); err != nil {
		log.Error("Failed to start MQTT broker", err)
		os.Exit(1)
	}
	defer broker.Stop()

	// Start web server
	webServer := web.NewServer(broker, log)
	go func() {
		if err := webServer.Start(*webPort); err != nil {
			log.Error("Failed to start web server", err)
			os.Exit(1)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down...")
}
