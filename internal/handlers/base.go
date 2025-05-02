package handlers

import (
	"google.golang.org/protobuf/proto"

	"github.com/mochi-mqtt/server/v2/packets"
)

// MessageHandler defines the interface for message handlers
type MessageHandler interface {
	// Type returns the type of messages this handler can process
	Type() string

	// Handle processes a message
	Handle(clientID string, packet packets.Packet) error

	// PublishResponse publishes a response message
	PublishResponse(clientID, topic string, msg proto.Message, qos byte, retain bool) error
}

// Server defines the interface for MQTT server operations
type Server interface {
	// Publish publishes a message to a topic
	Publish(clientID, topic string, payload []byte, qos byte, retain bool)
}

// BaseHandler provides common functionality for all handlers
type BaseHandler struct {
	server Server
}

// NewBaseHandler creates a new base handler
func NewBaseHandler(server Server) *BaseHandler {
	return &BaseHandler{
		server: server,
	}
}

// PublishResponse publishes a response message
func (h *BaseHandler) PublishResponse(clientID, topic string, msg proto.Message, qos byte, retain bool) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	h.server.Publish(clientID, topic, payload, qos, retain)
	return nil
}
