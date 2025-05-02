package hooks

import (
	"fmt"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/snac21/mqtt/internal/handlers"
	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// EventHook implements the mqtt.Hook interface for message handling
type EventHook struct {
	mqtt.HookBase
	handlers map[string]handlers.MessageHandler
	mu       sync.RWMutex
}

// NewEventHook creates a new event hook
func NewEventHook() *EventHook {
	return &EventHook{
		handlers: make(map[string]handlers.MessageHandler),
	}
}

// ID returns the unique identifier of the hook
func (h *EventHook) ID() string {
	return "event-hook"
}

// Provides indicates whether this hook provides the specified type
func (h *EventHook) Provides(b byte) bool {
	return true
}

// OnConnect handles client connections
func (h *EventHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	return nil
}

// OnPublish handles published messages
func (h *EventHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	// Parse base message
	var baseMsg pb.BaseMessage
	if err := proto.Unmarshal(pk.Payload, &baseMsg); err != nil {
		return pk, fmt.Errorf("failed to unmarshal base message from client %s: %w", cl.ID, err)
	}

	// Get handler for message type
	handler, ok := h.getHandler(baseMsg.Type)
	if !ok {
		return pk, fmt.Errorf("no handler found for message type %s from client %s", baseMsg.Type, cl.ID)
	}

	// Handle message
	if err := handler.Handle(cl.ID, &baseMsg); err != nil {
		return pk, fmt.Errorf("failed to handle message type %s from client %s: %w", baseMsg.Type, cl.ID, err)
	}

	return pk, nil
}

// OnSubscribe handles client subscriptions
func (h *EventHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnUnsubscribe handles client unsubscriptions
func (h *EventHook) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	return pk
}

// OnDisconnect handles client disconnections
func (h *EventHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	// Nothing to do here
}

// OnAuthPacket handles authentication packets
func (h *EventHook) OnAuthPacket(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	return pk, nil
}

// RegisterHandler registers a message handler
func (h *EventHook) RegisterHandler(handler handlers.MessageHandler) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.handlers[handler.Type()] = handler
}

// getHandler returns the handler for the specified message type
func (h *EventHook) getHandler(msgType string) (handlers.MessageHandler, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	handler, ok := h.handlers[msgType]
	return handler, ok
}
