package handlers

import (
	"github.com/mochi-mqtt/server/v2/packets"
	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// AuthHandler handles authentication messages
type AuthHandler struct {
	*BaseHandler
}

// NewAuthHandler creates a new authentication handler
func NewAuthHandler(server Server) *AuthHandler {
	return &AuthHandler{
		BaseHandler: NewBaseHandler(server),
	}
}

// Type returns the type of messages this handler can process
func (h *AuthHandler) Type() string {
	return "auth"
}

// Handle processes an authentication message
func (h *AuthHandler) Handle(clientID string, packet packets.Packet) error {
	// Parse base message
	var baseMsg pb.BaseMessage
	if err := proto.Unmarshal(packet.Payload, &baseMsg); err != nil {
		return err
	}

	// Parse auth message
	var authMsg pb.AuthMessage
	if err := proto.Unmarshal(baseMsg.Payload, &authMsg); err != nil {
		return err
	}

	// Process authentication request
	// TODO: Implement actual authentication logic

	// Create and publish response
	response := &pb.AuthMessage{
		Username:  authMsg.Username,
		Success:   true,
		Timestamp: authMsg.Timestamp,
	}

	// Create base message for response
	baseResp := &pb.BaseMessage{
		ClientId:  clientID,
		Timestamp: authMsg.Timestamp,
		Type:      "auth",
	}

	// Marshal auth response
	authPayload, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	baseResp.Payload = authPayload

	return h.PublishResponse(clientID, "auth/response", baseResp, packet.FixedHeader.Qos, packet.FixedHeader.Retain)
}
