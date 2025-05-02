package hooks

import (
	"strings"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/snac21/mqtt/internal/handlers"
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
	msgType := h.getMessageType(pk.TopicName)
	if handler, ok := h.getHandler(msgType); ok {
		if err := handler.Handle(cl.ID, pk); err != nil {
			return pk, err
		}
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

// getMessageType extracts the message type from the topic
func (h *EventHook) getMessageType(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}
