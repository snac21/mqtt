package handlers

import (
	"fmt"

	pb "github.com/snac21/mqtt/pkg/proto"
	"google.golang.org/protobuf/proto"
)

// DataHandler handles data messages
type DataHandler struct {
	BaseHandler
}

// NewDataHandler creates a new data handler
func NewDataHandler(server Server) *DataHandler {
	return &DataHandler{
		BaseHandler: *NewBaseHandler(server),
	}
}

// Type returns the type of messages this handler can process
func (h *DataHandler) Type() string {
	return "data"
}

// Handle processes a data message
func (h *DataHandler) Handle(clientID string, baseMsg *pb.BaseMessage) error {
	// Parse data message
	var dataMsg pb.DataMessage
	if err := proto.Unmarshal(baseMsg.Payload, &dataMsg); err != nil {
		return fmt.Errorf("failed to unmarshal data message: %w", err)
	}

	// Create response message
	response := &pb.BaseMessage{
		ClientId:  clientID,
		Type:      "data_ack",
		Timestamp: baseMsg.Timestamp,
		Metadata: map[string]string{
			"data_type": dataMsg.DataType,
		},
	}

	// Publish response
	return h.PublishResponse(clientID, fmt.Sprintf("response/%s", clientID), response, 0, false)
}
