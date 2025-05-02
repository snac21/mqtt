package handlers

import (
	"fmt"

	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// ControlHandler handles control messages
type ControlHandler struct {
	BaseHandler
}

// NewControlHandler creates a new control handler
func NewControlHandler(server Server) *ControlHandler {
	return &ControlHandler{
		BaseHandler: *NewBaseHandler(server),
	}
}

// Type returns the type of messages this handler can process
func (h *ControlHandler) Type() string {
	return "control"
}

// Handle processes a control message
func (h *ControlHandler) Handle(clientID string, baseMsg *pb.BaseMessage) error {
	// Parse control message
	var controlMsg pb.ControlMessage
	if err := proto.Unmarshal(baseMsg.Payload, &controlMsg); err != nil {
		return fmt.Errorf("failed to unmarshal control message: %w", err)
	}

	// Create response message
	response := &pb.BaseMessage{
		ClientId:  clientID,
		Type:      "control_ack",
		Timestamp: baseMsg.Timestamp,
		Metadata: map[string]string{
			"command": controlMsg.Command,
			"status":  controlMsg.Status,
		},
	}

	// Publish response
	return h.PublishResponse(clientID, fmt.Sprintf("response/%s", clientID), response, 0, false)
}
