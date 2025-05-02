package web

import (
	"github.com/gin-gonic/gin"
	"github.com/snac21/mqtt/logger"
)

// Server represents the web management server
type Server struct {
	router *gin.Engine
	logger *logger.Logger
}

// NewServer creates a new web server instance
func NewServer() *Server {
	router := gin.Default()
	server := &Server{
		router: router,
		logger: logger.New(),
	}

	// Setup routes
	server.setupRoutes()

	return server
}

// setupRoutes configures all API endpoints
func (s *Server) setupRoutes() {
	// Client management
	s.router.GET("/api/clients", s.getClients)
	s.router.GET("/api/clients/:id", s.getClient)
	s.router.DELETE("/api/clients/:id", s.deleteClient)

	// Topic management
	s.router.GET("/api/topics", s.getTopics)
	s.router.GET("/api/topics/:name", s.getTopic)

	// System metrics
	s.router.GET("/api/metrics", s.getMetrics)

	// Setup metrics routes
	s.SetupMetricsRoutes()
}

// Start runs the web server
func (s *Server) Start(addr string) error {
	s.logger.Info("Starting web server", "address", addr)
	return s.router.Run(addr)
}
