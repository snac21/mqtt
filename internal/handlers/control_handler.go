package handlers

import (
	"github.com/mochi-mqtt/server/v2/packets"
	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// ControlHandler handles control messages
type ControlHandler struct {
	*BaseHandler
}

// NewControlHandler creates a new control handler
func NewControlHandler(server Server) *ControlHandler {
	return &ControlHandler{
		BaseHandler: NewBaseHandler(server),
	}
}

// Type returns the type of messages this handler can process
func (h *ControlHandler) Type() string {
	return "control"
}

// Handle processes a control message
func (h *ControlHandler) Handle(clientID string, packet packets.Packet) error {
	// Parse base message
	var baseMsg pb.BaseMessage
	if err := proto.Unmarshal(packet.Payload, &baseMsg); err != nil {
		return err
	}

	// Parse control message
	var controlMsg pb.ControlMessage
	if err := proto.Unmarshal(baseMsg.Payload, &controlMsg); err != nil {
		return err
	}

	// Process control request
	// TODO: Implement actual control logic

	// Create and publish response
	response := &pb.ControlMessage{
		Command:   controlMsg.Command,
		Status:    "executed",
		Timestamp: controlMsg.Timestamp,
	}

	// Create base message for response
	baseResp := &pb.BaseMessage{
		ClientId:  clientID,
		Timestamp: controlMsg.Timestamp,
		Type:      "control",
	}

	// Marshal control response
	controlPayload, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	baseResp.Payload = controlPayload

	return h.PublishResponse(clientID, "control/response", baseResp, packet.FixedHeader.Qos, packet.FixedHeader.Retain)
}
