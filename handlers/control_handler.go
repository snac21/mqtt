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

// ControlHandler handles control messages
type ControlHandler struct {
	logger *logger.Logger
	server *mqtt.Server
}

// NewControlHandler creates a new ControlHandler instance
func NewControlHandler(s *mqtt.Server) *ControlHandler {
	return &ControlHandler{
		logger: logger.New(),
		server: s,
	}
}

// Type returns the message type this handler can process
func (h *ControlHandler) Type() string {
	return "control"
}

// Handle processes a control message
func (h *ControlHandler) Handle(ctx context.Context, client *mqtt.Client, packet *packets.Packet) error {
	// Parse the message payload
	var controlMsg proto.ControlMessage
	if err := pb.Unmarshal(packet.Payload, &controlMsg); err != nil {
		return fmt.Errorf("failed to parse control message: %v", err)
	}

	// Log the control message
	h.logger.Info("Control message received",
		"command", controlMsg.Command,
		"parameters", controlMsg.Parameters)

	// Process the control command
	response, err := h.processCommand(controlMsg)
	if err != nil {
		return fmt.Errorf("failed to process command: %v", err)
	}

	// Marshal response
	responseData, err := pb.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %v", err)
	}

	// Publish response
	responseTopic := fmt.Sprintf("control/response/%s", controlMsg.Command)
	if err := h.server.Publish(responseTopic, responseData, false, 0); err != nil {
		return fmt.Errorf("failed to publish response: %v", err)
	}

	return nil
}

// processCommand handles different control commands
func (h *ControlHandler) processCommand(msg proto.ControlMessage) (*proto.ControlMessage, error) {
	response := &proto.ControlMessage{
		Command: msg.Command,
		Parameters: map[string]string{
			"status": "success",
		},
	}

	switch msg.Command {
	case "restart":
		// TODO: Implement restart logic
		response.Parameters["message"] = "Device restart initiated"
	case "update":
		// TODO: Implement update logic
		response.Parameters["message"] = "Update process started"
	case "config":
		// TODO: Implement configuration update logic
		response.Parameters["message"] = "Configuration updated"
	default:
		response.Parameters["status"] = "error"
		response.Parameters["message"] = "Unknown command"
	}

	return response, nil
}
