package handlers

import (
	"context"
	"strings"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

// MessageHandler interface defines the contract for handling MQTT messages
type MessageHandler interface {
	// Handle processes an MQTT message
	Handle(ctx context.Context, client *mqtt.Client, packet *packets.Packet) error
	// Type returns the message type this handler can process
	Type() string
}

// EventHook implements mqtt.Hook interface for message processing
type EventHook struct {
	mqtt.HookBase
	handlers map[string]MessageHandler
}

// NewEventHook creates a new EventHook instance
func NewEventHook() *EventHook {
	return &EventHook{
		handlers: make(map[string]MessageHandler),
	}
}

// ID returns the hook ID
func (h *EventHook) ID() string {
	return "event-hook"
}

// Provides returns the hooks this hook provides
func (h *EventHook) Provides(byte) bool {
	return true
}

// OnPublish is called when a message is published
func (h *EventHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	// Get message type from topic or payload
	msgType := h.getMessageType(&pk)
	if handler, exists := h.handlers[msgType]; exists {
		ctx := context.Background()
		if err := handler.Handle(ctx, cl, &pk); err != nil {
			return pk, err
		}
	}

	return pk, nil
}

// RegisterHandler registers a new message handler
func (h *EventHook) RegisterHandler(handler MessageHandler) {
	h.handlers[handler.Type()] = handler
}

// getMessageType extracts the message type from the publish packet
func (h *EventHook) getMessageType(pk *packets.Packet) string {
	// Extract message type from topic
	// Topic format: "type/..."
	parts := strings.Split(pk.TopicName, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return "default"
}
