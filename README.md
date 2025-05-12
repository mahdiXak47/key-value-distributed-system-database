مس# Distributed Key-Value Store System

A distributed key-value store system with load balancing, cluster management, and a modern web interface.

## System Components

The system consists of three main components:

1. **Controller** (`/controller`)
   - Cluster management dashboard
   - Node health monitoring
   - Partition management
   - Leader election
   - Real-time status updates

2. **Load Balancer** (`/load-balancer`)
   - Round-robin load balancing
   - Health check monitoring
   - Dynamic node management
   - Request forwarding
   - Automatic failover

3. **Client** (`/client`)
   - Modern web interface
   - Key-value operations (Set, Get, Delete)
   - Automatic retry mechanism
   - Responsive design

## Architecture

```
┌─────────┐     ┌──────────────┐     ┌─────────────┐
│ Client  │────▶│ Load Balancer│────▶│ Controller  │
└─────────┘     └──────────────┘     └─────────────┘
                      │
                      ▼
                ┌─────────────┐
                │    Nodes    │
                └─────────────┘
```

## Getting Started

### Prerequisites

- Go 1.16 or higher
- Modern web browser

### Running the System

1. Start the Controller:
   ```bash
   cd controller
   go run main.go
   ```
   The controller will be available at `http://localhost:8080`

2. Start the Load Balancer:
   ```bash
   cd load-balancer
   go run main.go
   ```
   The load balancer will be available at `http://localhost:8081`

3. Start the Client:
   ```bash
   cd client
   go run main.go
   ```
   The client interface will be available at `http://localhost:8082`

## Features

### Controller
- Cluster management dashboard
- Node health monitoring
- Partition management
- Leader election
- Real-time status updates

### Load Balancer
- Round-robin load balancing
- Health check monitoring
- Dynamic node management
- Request forwarding
- Automatic failover

### Client
- Modern web interface
- Key-value operations
- Automatic retry mechanism
- Responsive design

## Directory Structure

```
.
├── controller/           # Cluster management system
│   ├── main.go
│   ├── handlers/
│   ├── templates/
│   └── static/
├── load-balancer/       # Load balancing system
│   └── main.go
├── client/             # Client application
│   ├── main.go
│   ├── templates/
│   └── static/
└── README.md
```

## API Documentation

### Controller API
- See `controller/README.md` for detailed API documentation

### Load Balancer API
- See `load-balancer/README.md` for detailed API documentation

### Client API
- See `client/README.md` for detailed API documentation

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.