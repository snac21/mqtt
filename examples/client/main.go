package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/snac21/mqtt/pkg/proto"
	pb "google.golang.org/protobuf/proto"
)

func main() {
	// Parse command line flags
	broker := flag.String("broker", "tcp://localhost:1883", "MQTT broker address")
	clientID := flag.String("client-id", "test-client", "Client ID")
	username := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")
	flag.Parse()

	// Create MQTT client options
	opts := mqtt.NewClientOptions().
		AddBroker(*broker).
		SetClientID(*clientID).
		SetUsername(*username).
		SetPassword(*password).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Printf("Connection lost: %v", err)
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			log.Println("Connected to broker")
		})

	// Create and connect client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to connect: %v", token.Error())
	}
	defer client.Disconnect(250)

	// Subscribe to response topics
	if token := client.Subscribe("response/#", 0, func(client mqtt.Client, msg mqtt.Message) {
		var response proto.BaseMessage
		if err := pb.Unmarshal(msg.Payload(), &response); err != nil {
			log.Printf("Failed to parse response: %v", err)
			return
		}
		log.Printf("Received response from client %s", response.ClientId)
	}); token.Wait() && token.Error() != nil {
		log.Fatalf("Failed to subscribe: %v", token.Error())
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start publishing messages
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Create status message
				statusMsg := &proto.StatusMessage{
					ClientId: *clientID,
					Status:   "online",
					Metrics: map[string]string{
						"battery": "100",
						"signal":  "90",
					},
					Timestamp: time.Now().Unix(),
				}

				// Marshal message
				data, err := pb.Marshal(statusMsg)
				if err != nil {
					log.Printf("Failed to marshal message: %v", err)
					continue
				}

				// Publish message
				topic := fmt.Sprintf("status/%s", *clientID)
				if token := client.Publish(topic, 0, false, data); token.Wait() && token.Error() != nil {
					log.Printf("Failed to publish message: %v", token.Error())
				}
			}
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
}
