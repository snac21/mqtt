package handlers

import (
	"fmt"

	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// StatusHandler handles status messages
type StatusHandler struct {
	BaseHandler
}

// NewStatusHandler creates a new status handler
func NewStatusHandler(server Server) *StatusHandler {
	return &StatusHandler{
		BaseHandler: *NewBaseHandler(server),
	}
}

// Type returns the type of messages this handler can process
func (h *StatusHandler) Type() string {
	return "status"
}

// Handle processes a status message
func (h *StatusHandler) Handle(clientID string, baseMsg *pb.BaseMessage) error {
	// Parse status message
	var statusMsg pb.StatusMessage
	if err := proto.Unmarshal(baseMsg.Payload, &statusMsg); err != nil {
		return fmt.Errorf("failed to unmarshal status message: %w", err)
	}

	// Create response message
	response := &pb.BaseMessage{
		ClientId:  clientID,
		Type:      "status_ack",
		Timestamp: baseMsg.Timestamp,
		Metadata: map[string]string{
			"status": statusMsg.Status,
		},
	}

	// Publish response
	return h.PublishResponse(clientID, fmt.Sprintf("response/%s", clientID), response, 0, false)
}
