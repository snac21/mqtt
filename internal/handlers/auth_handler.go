package handlers

import (
	"fmt"

	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// AuthHandler handles authentication messages
type AuthHandler struct {
	BaseHandler
}

// NewAuthHandler creates a new auth handler
func NewAuthHandler(server Server) *AuthHandler {
	return &AuthHandler{
		BaseHandler: *NewBaseHandler(server),
	}
}

// Type returns the type of messages this handler can process
func (h *AuthHandler) Type() string {
	return "auth"
}

// Handle processes an auth message
func (h *AuthHandler) Handle(clientID string, baseMsg *pb.BaseMessage) error {
	// Parse auth message
	var authMsg pb.AuthMessage
	if err := proto.Unmarshal(baseMsg.Payload, &authMsg); err != nil {
		return fmt.Errorf("failed to unmarshal auth message: %w", err)
	}

	// Create response message
	response := &pb.BaseMessage{
		ClientId:  clientID,
		Type:      "auth_ack",
		Timestamp: baseMsg.Timestamp,
		Metadata: map[string]string{
			"success": fmt.Sprintf("%v", authMsg.Success),
		},
	}

	// Publish response
	return h.PublishResponse(clientID, fmt.Sprintf("response/%s", clientID), response, 0, false)
}
