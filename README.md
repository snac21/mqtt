# MQTT Broker

A high-performance MQTT broker with support for InfluxDB storage, Nacos service discovery, and web management interface.

## Features

- MQTT v3.1.1 and v5.0 support
- Authentication and authorization
- Message persistence using InfluxDB
- Service discovery using Nacos
- Web management interface
- Metrics collection and monitoring
- Protocol buffer message definitions
- Structured logging with rotation

## Requirements

- Go 1.21 or later
- Protocol Buffers compiler
- InfluxDB 2.x
- Nacos server

## Installation

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

4. Build the broker:
```bash
make build
```

## Configuration

Create a `config.yaml` file in the `config` directory:

```yaml
broker:
  port: 1883
  auth:
    enabled: false
    username: ""
    password: ""

web:
  port: ":8080"

discovery:
  address: "localhost"
  port: 8848

storage:
  url: "http://localhost:8086"
  token: "your-influxdb-token"
  organization: "mqtt"
  bucket: "mqtt"

logger:
  level: "info"
  format: "json"
  output: "logs/mqtt-broker.log"
```

## Usage

1. Start the broker:
```bash
make run
```

2. Access the web interface:
```
http://localhost:8080
```

## API Endpoints

- `/api/metrics` - Get broker metrics
- `/api/clients` - List connected clients
- `/api/topics` - List active topics
- `/health` - Health check endpoint

## Development

1. Run tests:
```bash
make test
```

2. Run linter:
```bash
make lint
```

3. Clean build artifacts:
```bash
make clean
```

## Project Structure

```
mqtt/
├── cmd/                    # Application entry points
│   └── broker/            # MQTT broker application
├── internal/              # Internal packages
│   ├── broker/           # Broker core implementation
│   ├── config/           # Configuration management
│   ├── discovery/        # Service discovery
│   ├── handlers/         # MQTT message handlers
│   ├── logger/           # Logging
│   ├── storage/          # Storage implementation
│   └── web/              # Web API implementation
├── pkg/                   # Public packages
│   ├── proto/            # Protocol buffer definitions
│   └── mqtt/             # MQTT common interfaces
└── examples/             # Example code
    └── client/           # Client examples
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 


### 基本操作
```
在 mochi-mqtt v2.7.9 中，一个标准的 Hook 实现需要：
组合 mqtt.HookBase
实现 Hook 接口的所有方法，包括：
ID() string
Provides(b byte) bool
OnConnect(cl *mqtt.Client, pk packets.Packet) error
OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error)
OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet
OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet
OnDisconnect(cl *mqtt.Client, err error, expire bool)
OnAuthPacket(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error)
让我们修改 InfluxDBHook：
```