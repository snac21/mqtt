# MQTT Broker Implementation

This project implements an MQTT broker based on the mochi-mqtt server library with additional features including distributed storage, service discovery, and web management interface.

## Architecture

### Core Components

1. **MQTT Broker**
   - Based on mochi-mqtt/server v2.7.9
   - Handles MQTT protocol operations
   - Implements custom hooks for message processing
   - Supports authentication and message handling

2. **Message Handlers**
   - Interface-based design for message processing
   - Protocol Buffers for message serialization
   - Type-based handler routing
   - Support for auth_handler and message_handler

3. **Storage Layer**
   - InfluxDB for time-series data storage
   - Raft consensus for distributed storage
   - Custom storage hooks implementation

4. **Service Discovery**
   - R-Nacos as primary service registry
   - Consul as fallback option
   - Service health monitoring

5. **Web Management Interface**
   - Gin-based HTTP server
   - MQTT client management
   - Server monitoring dashboard
   - Real-time metrics

6. **Logging**
   - Zap logger implementation
   - Structured logging
   - Log rotation and management

### Project Structure

```
mqtt-clz/
├── broker/           # MQTT broker implementation
├── handlers/         # Message handlers
├── storage/          # Storage implementations
├── discovery/        # Service discovery
├── web/             # Web management interface
├── proto/           # Protocol buffer definitions
├── config/          # Configuration management
├── logger/          # Logging implementation
└── cmd/             # Command line applications
```

## Getting Started

### Prerequisites

- Go 1.19+
- InfluxDB
- R-Nacos or Consul
- Make

### Installation

1. Clone the repository:
```bash
git clone https://github.com/snac21/mqtt.git
cd mqtt
```

2. Install dependencies:
```bash
make deps
```

3. Generate protocol buffer code:
```bash
make proto
```

4. Build the project:
```bash
make build
```

### Configuration

Configuration is managed through environment variables and configuration files. See `config/` directory for details.

## Development

### Building

```bash
make build
```

### Running Tests

```bash
make test
```

### Generating Protocol Buffers

```bash
make proto
```

## License

MIT License 