package handlers

import (
	"context"
	"fmt"

	"gitee.com/snac21/mqtt/logger"
	"gitee.com/snac21/mqtt/proto"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	pb "google.golang.org/protobuf/proto"
)

// StatusHandler handles status messages
type StatusHandler struct {
	logger *logger.Logger
	server *mqtt.Server
}

// NewStatusHandler creates a new StatusHandler instance
func NewStatusHandler(server *mqtt.Server) *StatusHandler {
	return &StatusHandler{
		logger: logger.New(),
		server: server,
	}
}

// Type returns the message type this handler can process
func (h *StatusHandler) Type() string {
	return "status"
}

// Handle processes a status message
func (h *StatusHandler) Handle(ctx context.Context, client *mqtt.Client, packet *packets.Packet) error {
	// Parse the message payload
	var statusMsg proto.StatusMessage
	if err := pb.Unmarshal(packet.Payload, &statusMsg); err != nil {
		return fmt.Errorf("failed to parse status message: %v", err)
	}

	// Log the status message
	h.logger.Info("Status message received",
		"device_id", statusMsg.DeviceId,
		"status", statusMsg.Status,
		"timestamp", statusMsg.Timestamp,
		"metrics", statusMsg.Metrics)

	// Create acknowledgment message
	ack := &proto.StatusMessage{
		DeviceId:  statusMsg.DeviceId,
		Status:    "received",
		Timestamp: statusMsg.Timestamp,
	}

	// Marshal acknowledgment
	ackData, err := pb.Marshal(ack)
	if err != nil {
		return fmt.Errorf("failed to marshal acknowledgment: %v", err)
	}

	// Publish acknowledgment
	ackTopic := fmt.Sprintf("status/ack/%s", statusMsg.DeviceId)
	if err := h.server.Publish(ackTopic, ackData, false, 0); err != nil {
		return fmt.Errorf("failed to publish acknowledgment: %v", err)
	}

	return nil
}
