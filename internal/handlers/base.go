package handlers

import (
	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// MessageHandler defines the interface for message handlers
type MessageHandler interface {
	// Type returns the type of messages this handler can process
	Type() string

	// Handle processes a message
	Handle(clientID string, baseMsg *pb.BaseMessage) error

	// PublishResponse publishes a response message
	PublishResponse(clientID, topic string, msg *pb.BaseMessage, qos byte, retain bool) error
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
func (h *BaseHandler) PublishResponse(clientID, topic string, msg *pb.BaseMessage, qos byte, retain bool) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	h.server.Publish(clientID, topic, payload, qos, retain)
	return nil
}
