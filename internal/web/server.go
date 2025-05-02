package web

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/snac21/mqtt/internal/broker"
	"github.com/snac21/mqtt/internal/logger"
)

// Server represents the web server
type Server struct {
	broker *broker.Broker
	logger *logger.Logger
	router *gin.Engine
}

// NewServer creates a new web server instance
func NewServer(broker *broker.Broker, logger *logger.Logger) *Server {
	s := &Server{
		broker: broker,
		logger: logger,
		router: gin.Default(),
	}

	s.setupRoutes()
	return s
}

// Start starts the web server
func (s *Server) Start(addr string) error {
	s.logger.Info("Starting web server", map[string]interface{}{"addr": addr})
	return s.router.Run(addr)
}

// setupRoutes sets up the HTTP routes
func (s *Server) setupRoutes() {
	// API routes
	api := s.router.Group("/api")
	{
		api.GET("/metrics", s.handleMetrics)
		api.GET("/clients", s.handleListClients)
		api.GET("/topics", s.handleListTopics)
	}

	// Health check
	s.router.GET("/health", s.handleHealth)
}

// handleMetrics handles the metrics endpoint
func (s *Server) handleMetrics(c *gin.Context) {
	metrics := s.broker.GetMetrics()
	c.JSON(http.StatusOK, metrics)
}

// handleListClients handles the clients endpoint
func (s *Server) handleListClients(c *gin.Context) {
	clients := s.broker.GetClients()
	c.JSON(http.StatusOK, clients)
}

// handleListTopics handles the topics endpoint
func (s *Server) handleListTopics(c *gin.Context) {
	topics := s.broker.GetTopics()
	c.JSON(http.StatusOK, topics)
}

// handleHealth handles the health check endpoint
func (s *Server) handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ok",
	})
}
