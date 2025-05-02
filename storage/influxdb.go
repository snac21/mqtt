package storage

import (
	"fmt"
	"time"

	"gitee.com/snac21/mqtt/logger"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// InfluxDBStorage implements mqtt.Hook interface for message storage
type InfluxDBStorage struct {
	mqtt.HookBase
	client   influxdb2.Client
	writeAPI api.WriteAPI
	org      string
	bucket   string
	logger   *logger.Logger
}

// NewInfluxDBStorage creates a new InfluxDB storage instance
func NewInfluxDBStorage(url, token, org, bucket string) (*InfluxDBStorage, error) {
	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPI(org, bucket)

	// Create storage instance
	storage := &InfluxDBStorage{
		client:   client,
		writeAPI: writeAPI,
		org:      org,
		bucket:   bucket,
		logger:   logger.New(),
	}

	// Handle write errors
	go func() {
		for err := range writeAPI.Errors() {
			fmt.Printf("InfluxDB write error: %v\n", err)
		}
	}()

	return storage, nil
}

// ID returns the hook ID
func (h *InfluxDBStorage) ID() string {
	return "influxdb-storage"
}

// Provides returns the hooks this hook provides
func (h *InfluxDBStorage) Provides(byte) bool {
	return true
}

// OnConnect is called when a client connects
func (h *InfluxDBStorage) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	// Create point for the connection
	point := write.NewPoint(
		"mqtt_connection",
		map[string]string{
			"client_id": cl.ID,
		},
		map[string]interface{}{
			"qos":    pk.FixedHeader.Qos,
			"dup":    pk.FixedHeader.Dup,
			"retain": pk.FixedHeader.Retain,
			"type":   pk.FixedHeader.Type,
		},
		time.Now(),
	)

	// Write the point
	h.writeAPI.WritePoint(point)

	return nil
}

// OnPublish is called when a message is published
func (h *InfluxDBStorage) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	// Create point for the message
	point := write.NewPoint(
		"mqtt_message",
		map[string]string{
			"client_id": cl.ID,
			"topic":     pk.TopicName,
		},
		map[string]interface{}{
			"payload": string(pk.Payload),
			"qos":     pk.FixedHeader.Qos,
		},
		time.Now(),
	)

	// Write the point
	h.writeAPI.WritePoint(point)

	return pk, nil
}

// OnSubscribe is called when a client subscribes to a topic
func (h *InfluxDBStorage) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	// Create point for the subscription
	point := write.NewPoint(
		"mqtt_subscription",
		map[string]string{
			"client_id": cl.ID,
			"topic":     pk.TopicName,
		},
		map[string]interface{}{
			"qos": pk.FixedHeader.Qos,
		},
		time.Now(),
	)

	// Write the point
	h.writeAPI.WritePoint(point)

	return pk
}

// OnUnsubscribe is called when a client unsubscribes from a topic
func (h *InfluxDBStorage) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	// Create point for the unsubscription
	point := write.NewPoint(
		"mqtt_unsubscription",
		map[string]string{
			"client_id": cl.ID,
			"topic":     pk.TopicName,
		},
		nil,
		time.Now(),
	)

	// Write the point
	h.writeAPI.WritePoint(point)

	return pk
}

// OnDisconnect is called when a client disconnects
func (h *InfluxDBStorage) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	// Create point for the disconnection
	point := write.NewPoint(
		"mqtt_disconnection",
		map[string]string{
			"client_id": cl.ID,
		},
		map[string]interface{}{
			"error":  err != nil,
			"expire": expire,
		},
		time.Now(),
	)

	// Write the point
	h.writeAPI.WritePoint(point)
}

// Close closes the InfluxDB client
func (h *InfluxDBStorage) Close() {
	h.writeAPI.Flush()
	h.client.Close()
}
