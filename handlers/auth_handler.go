package handlers

import (
	"context"
	"fmt"
	"log"

	"gitee.com/snac21/mqtt/proto"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	pb "google.golang.org/protobuf/proto"
)

// AuthHandler handles authentication messages
type AuthHandler struct {
	logger *log.Logger
	server *mqtt.Server
}

// NewAuthHandler creates a new AuthHandler instance
func NewAuthHandler(s *mqtt.Server) *AuthHandler {
	return &AuthHandler{
		logger: log.New(log.Writer(), "[Auth] ", log.LstdFlags),
		server: s,
	}
}

// Type returns the message type this handler can process
func (h *AuthHandler) Type() string {
	return "auth"
}

// Handle processes an authentication message
func (h *AuthHandler) Handle(ctx context.Context, client *mqtt.Client, packet *packets.Packet) error {
	// Parse the message payload
	var authMsg proto.AuthMessage
	if err := pb.Unmarshal(packet.Payload, &authMsg); err != nil {
		return fmt.Errorf("failed to parse auth message: %v", err)
	}

	// Log the authentication attempt
	h.logger.Printf("Authentication attempt - client_id: %s, username: %s, success: %v",
		authMsg.ClientId, authMsg.Username, authMsg.Success)

	// TODO: Implement actual authentication logic
	// This could involve checking against a database, external service, etc.

	// Create response message
	response := &proto.AuthMessage{
		ClientId: authMsg.ClientId,
		Success:  true, // TODO: Set based on actual authentication result
	}

	// Marshal response
	responseData, err := pb.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal auth response: %v", err)
	}

	// Publish response
	responseTopic := fmt.Sprintf("auth/response/%s", authMsg.ClientId)
	if err := h.server.Publish(responseTopic, responseData, false, 0); err != nil {
		return fmt.Errorf("failed to publish auth response: %v", err)
	}

	return nil
}
