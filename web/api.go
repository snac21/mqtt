package web

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Client represents an MQTT client
type Client struct {
	ID        string `json:"id"`
	Username  string `json:"username"`
	Connected bool   `json:"connected"`
	IP        string `json:"ip"`
}

// Topic represents an MQTT topic
type Topic struct {
	Name        string `json:"name"`
	Subscribers int    `json:"subscribers"`
}

// Metrics represents system metrics
type Metrics struct {
	ClientsConnected int `json:"clients_connected"`
	TopicsActive     int `json:"topics_active"`
	MessagesReceived int `json:"messages_received"`
	MessagesSent     int `json:"messages_sent"`
}

// getClients returns a list of connected MQTT clients
func (s *Server) getClients(c *gin.Context) {
	// TODO: Get actual client list from MQTT server
	clients := []Client{
		{
			ID:        "client1",
			Username:  "user1",
			Connected: true,
			IP:        "127.0.0.1",
		},
	}
	c.JSON(http.StatusOK, gin.H{"clients": clients})
}

// getClient returns details of a specific client
func (s *Server) getClient(c *gin.Context) {
	clientID := c.Param("id")
	// TODO: Get actual client details from MQTT server
	client := Client{
		ID:        clientID,
		Username:  "user1",
		Connected: true,
		IP:        "127.0.0.1",
	}
	c.JSON(http.StatusOK, client)
}

// deleteClient disconnects a client
func (s *Server) deleteClient(c *gin.Context) {
	clientID := c.Param("id")
	// TODO: Implement client disconnection
	s.logger.Info("Client disconnected", "client_id", clientID)
	c.Status(http.StatusNoContent)
}

// getTopics returns a list of active topics
func (s *Server) getTopics(c *gin.Context) {
	// TODO: Get actual topic list from MQTT server
	topics := []Topic{
		{
			Name:        "test/topic",
			Subscribers: 2,
		},
	}
	c.JSON(http.StatusOK, gin.H{"topics": topics})
}

// getTopic returns details of a specific topic
func (s *Server) getTopic(c *gin.Context) {
	topicName := c.Param("name")
	// TODO: Get actual topic details from MQTT server
	topic := Topic{
		Name:        topicName,
		Subscribers: 1,
	}
	c.JSON(http.StatusOK, topic)
}

// getMetrics returns system metrics
func (s *Server) getMetrics(c *gin.Context) {
	// TODO: Get actual metrics from MQTT server
	metrics := Metrics{
		ClientsConnected: 5,
		TopicsActive:     10,
		MessagesReceived: 1000,
		MessagesSent:     800,
	}
	c.JSON(http.StatusOK, metrics)
}
