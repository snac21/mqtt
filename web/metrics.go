package web

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/snac21/mqtt/logger"
)

// MetricsCollector collects and exposes MQTT broker metrics
type MetricsCollector struct {
	clientsConnected     prometheus.Gauge
	topicsActive         prometheus.Gauge
	messagesReceived     prometheus.Counter
	messagesSent         prometheus.Counter
	messageLatency       prometheus.Histogram
	clientConnections    prometheus.Counter
	clientDisconnections prometheus.Counter
	logger               *logger.Logger
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		clientsConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_clients_connected",
			Help: "Number of currently connected MQTT clients",
		}),
		topicsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "mqtt_topics_active",
			Help: "Number of active MQTT topics",
		}),
		messagesReceived: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_messages_received_total",
			Help: "Total number of MQTT messages received",
		}),
		messagesSent: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_messages_sent_total",
			Help: "Total number of MQTT messages sent",
		}),
		messageLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mqtt_message_latency_seconds",
			Help:    "Message processing latency in seconds",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
		}),
		clientConnections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_client_connections_total",
			Help: "Total number of client connections",
		}),
		clientDisconnections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mqtt_client_disconnections_total",
			Help: "Total number of client disconnections",
		}),
		logger: logger.New(),
	}
}

// RegisterMetrics registers the metrics with Prometheus
func (m *MetricsCollector) RegisterMetrics() {
	prometheus.MustRegister(
		m.clientsConnected,
		m.topicsActive,
		m.messagesReceived,
		m.messagesSent,
		m.messageLatency,
		m.clientConnections,
		m.clientDisconnections,
	)
}

// UpdateClientCount updates the number of connected clients
func (m *MetricsCollector) UpdateClientCount(count int) {
	m.clientsConnected.Set(float64(count))
}

// UpdateTopicCount updates the number of active topics
func (m *MetricsCollector) UpdateTopicCount(count int) {
	m.topicsActive.Set(float64(count))
}

// IncrementMessagesReceived increments the received messages counter
func (m *MetricsCollector) IncrementMessagesReceived() {
	m.messagesReceived.Inc()
}

// IncrementMessagesSent increments the sent messages counter
func (m *MetricsCollector) IncrementMessagesSent() {
	m.messagesSent.Inc()
}

// RecordMessageLatency records the processing time of a message
func (m *MetricsCollector) RecordMessageLatency(duration time.Duration) {
	m.messageLatency.Observe(duration.Seconds())
}

// IncrementClientConnections increments the client connections counter
func (m *MetricsCollector) IncrementClientConnections() {
	m.clientConnections.Inc()
}

// IncrementClientDisconnections increments the client disconnections counter
func (m *MetricsCollector) IncrementClientDisconnections() {
	m.clientDisconnections.Inc()
}

// SetupMetricsRoutes sets up the metrics endpoints
func (s *Server) SetupMetricsRoutes() {
	// Create metrics collector
	collector := NewMetricsCollector()
	collector.RegisterMetrics()

	// Add metrics endpoint
	s.router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Add health check endpoint
	s.router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status": "healthy",
			"time":   time.Now().UTC(),
		})
	})
}
