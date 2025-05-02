package hooks

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// InfluxDBHook implements the MQTT server Hook interface for storing messages
type InfluxDBHook struct {
	mqtt.HookBase
	client   influxdb2.Client
	writeAPI api.WriteAPIBlocking
	org      string
	bucket   string
}

// NewInfluxDBHook creates a new InfluxDB hook instance
func NewInfluxDBHook(url, token, org, bucket string) (*InfluxDBHook, error) {
	client := influxdb2.NewClient(url, token)
	writeAPI := client.WriteAPIBlocking(org, bucket)

	return &InfluxDBHook{
		client:   client,
		writeAPI: writeAPI,
		org:      org,
		bucket:   bucket,
	}, nil
}

// ID returns the unique identifier of the hook
func (h *InfluxDBHook) ID() string {
	return "influxdb-hook"
}

// Provides indicates whether this hook provides the specified type
func (h *InfluxDBHook) Provides(b byte) bool {
	return true
}

// OnConnect handles client connections
func (h *InfluxDBHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	point := influxdb2.NewPoint(
		"mqtt_connections",
		map[string]string{
			"client_id": cl.ID,
			"username":  string(cl.Properties.Username),
		},
		map[string]interface{}{
			"connected": true,
		},
		time.Now(),
	)

	if err := h.writeAPI.WritePoint(context.Background(), point); err != nil {
		return fmt.Errorf("failed to write connection point: %w", err)
	}

	return nil
}

// OnPublish handles published messages
func (h *InfluxDBHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	point := influxdb2.NewPoint(
		"mqtt_messages",
		map[string]string{
			"client_id": cl.ID,
			"topic":     pk.TopicName,
		},
		map[string]interface{}{
			"payload": string(pk.Payload),
			"qos":     pk.FixedHeader.Qos,
			"retain":  pk.FixedHeader.Retain,
		},
		time.Now(),
	)

	if err := h.writeAPI.WritePoint(context.Background(), point); err != nil {
		return pk, fmt.Errorf("failed to write message point: %w", err)
	}

	return pk, nil
}

// OnSubscribe handles client subscriptions
func (h *InfluxDBHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	point := influxdb2.NewPoint(
		"mqtt_subscriptions",
		map[string]string{
			"client_id": cl.ID,
		},
		map[string]interface{}{
			"topic_filters": len(pk.Filters),
		},
		time.Now(),
	)

	if err := h.writeAPI.WritePoint(context.Background(), point); err != nil {
		// Since we can't return an error, we'll just log it
		return pk
	}

	return pk
}

// OnUnsubscribe handles client unsubscriptions
func (h *InfluxDBHook) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnDisconnect handles client disconnections
func (h *InfluxDBHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	point := influxdb2.NewPoint(
		"mqtt_connections",
		map[string]string{
			"client_id": cl.ID,
			"username":  string(cl.Properties.Username),
		},
		map[string]interface{}{
			"connected": false,
			"error":     err != nil,
			"expired":   expire,
		},
		time.Now(),
	)

	if writeErr := h.writeAPI.WritePoint(context.Background(), point); writeErr != nil {
		// Since we can't return an error, we'll just ignore it
		return
	}
}

// OnAuthPacket handles authentication packets
func (h *InfluxDBHook) OnAuthPacket(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	return pk, nil
}

// Close closes the InfluxDB connection
func (h *InfluxDBHook) Close() {
	h.client.Close()
}
