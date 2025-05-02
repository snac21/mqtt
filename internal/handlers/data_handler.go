package handlers

import (
	"github.com/mochi-mqtt/server/v2/packets"
	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// DataHandler handles data messages
type DataHandler struct {
	*BaseHandler
}

// NewDataHandler creates a new data handler
func NewDataHandler(server Server) *DataHandler {
	return &DataHandler{
		BaseHandler: NewBaseHandler(server),
	}
}

// Type returns the type of messages this handler can process
func (h *DataHandler) Type() string {
	return "data"
}

// Handle processes a data message
func (h *DataHandler) Handle(clientID string, packet packets.Packet) error {
	// Parse base message
	var baseMsg pb.BaseMessage
	if err := proto.Unmarshal(packet.Payload, &baseMsg); err != nil {
		return err
	}

	// Parse data message
	var dataMsg pb.DataMessage
	if err := proto.Unmarshal(baseMsg.Payload, &dataMsg); err != nil {
		return err
	}

	// Process data message
	// TODO: Implement actual data processing logic

	// Create and publish response
	response := &pb.DataMessage{
		ClientId:  clientID,
		DataType:  dataMsg.DataType,
		Payload:   dataMsg.Payload,
		Metadata:  dataMsg.Metadata,
		Timestamp: dataMsg.Timestamp,
	}

	// Create base message for response
	baseResp := &pb.BaseMessage{
		ClientId:  clientID,
		Timestamp: dataMsg.Timestamp,
		Type:      "data",
	}

	// Marshal data response
	dataPayload, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	baseResp.Payload = dataPayload

	return h.PublishResponse(clientID, "data/response", baseResp, packet.FixedHeader.Qos, packet.FixedHeader.Retain)
}
