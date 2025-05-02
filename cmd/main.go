package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"gitee.com/snac21/mqtt/broker"
	"gitee.com/snac21/mqtt/discovery"
	"gitee.com/snac21/mqtt/logger"
	"gitee.com/snac21/mqtt/web"
)

func main() {
	// Parse command line flags
	port := flag.Int("port", 1883, "MQTT broker port")
	webPort := flag.String("web-port", ":8080", "Web management interface port")
	nacosAddr := flag.String("nacos-addr", "localhost", "Nacos server address")
	nacosPort := flag.Uint64("nacos-port", 8848, "Nacos server port")
	influxURL := flag.String("influx-url", "http://localhost:8086", "InfluxDB URL")
	influxToken := flag.String("influx-token", "", "InfluxDB token")
	influxOrg := flag.String("influx-org", "mqtt", "InfluxDB organization")
	influxBucket := flag.String("influx-bucket", "mqtt", "InfluxDB bucket")
	flag.Parse()

	// Initialize logger
	log := logger.New()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize Nacos discovery
	discovery, err := discovery.NewNacosDiscovery(*nacosAddr, *nacosPort)
	if err != nil {
		log.Error("Failed to initialize Nacos discovery", err)
		os.Exit(1)
	}

	// Create and start MQTT broker
	broker, err := broker.New(&broker.Config{
		Port:         *port,
		Auth:         false, // TODO: Make configurable
		Username:     "",
		Password:     "",
		InfluxURL:    *influxURL,
		InfluxToken:  *influxToken,
		InfluxOrg:    *influxOrg,
		InfluxBucket: *influxBucket,
	})
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

	// Register with Nacos
	if err := discovery.RegisterService("mqtt-broker", "localhost", uint64(*port)); err != nil {
		log.Error("Failed to register with Nacos", err)
		os.Exit(1)
	}
	defer discovery.DeregisterService("mqtt-broker", "localhost", uint64(*port))

	// Start web server
	webServer := web.NewServer()
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
