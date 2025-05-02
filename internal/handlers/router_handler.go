package handlers

import (
	"fmt"
	"sync"

	"github.com/mochi-mqtt/server/v2/packets"
	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// Router handles message routing to appropriate handlers
type Router struct {
	handlers map[string]MessageHandler
	mu       sync.RWMutex
}

// NewRouter creates a new router instance
func NewRouter() *Router {
	return &Router{
		handlers: make(map[string]MessageHandler),
	}
}

// RegisterHandler registers a message handler
func (r *Router) RegisterHandler(handler MessageHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[handler.Type()] = handler
}

// Handle processes a message and routes it to the appropriate handler
func (r *Router) Handle(clientID string, packet packets.Packet) error {
	// Parse base message
	var baseMsg pb.BaseMessage
	if err := proto.Unmarshal(packet.Payload, &baseMsg); err != nil {
		return fmt.Errorf("failed to unmarshal base message: %w", err)
	}

	// Get handler for message type
	r.mu.RLock()
	handler, ok := r.handlers[baseMsg.Type]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no handler found for message type: %s", baseMsg.Type)
	}

	// Process message with handler
	return handler.Handle(clientID, &baseMsg)
}
