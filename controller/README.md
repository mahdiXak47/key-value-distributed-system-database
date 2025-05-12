# Key-Value Store Controller

A management interface for the distributed key-value store system that handles cluster management, node health monitoring, and partition management.

## Features

- Cluster Management Dashboard
- Node Management:
  - Add/Remove nodes
  - Monitor node health
  - View node status
- Partition Management:
  - Add/Remove partitions
  - Transfer partitions between nodes
  - Change partition leaders
- Automatic Health Checks
- Leader Election
- Real-time Status Updates

## Architecture

The controller application consists of:
- Cluster management system
- Health check monitoring
- Web interface for cluster management
- Partition management system
- Leader election mechanism

## Technical Details

- Built with Go
- Uses HTTP/JSON for communication
- Implements health check system
- Handles leader election
- Manages partition distribution
- Provides web-based management interface

## Running the Application

1. Make sure you have Go installed
2. Navigate to the controller directory:
   ```bash
   cd controller
   ```
3. Run the application:
   ```bash
   go run main.go
   ```
4. Access the management interface at `http://localhost:8080`

## Configuration

The controller runs on port 8080 by default. You can modify this in `main.go`.

## Directory Structure

```
controller/
├── main.go           # Main application code
├── cluster/          # Cluster management
│   └── cluster.go    # Cluster implementation
├── handlers/         # HTTP handlers
│   └── handlers.go   # Request handlers
├── templates/        # HTML templates
│   └── index.html    # Dashboard template
└── static/          # Static assets
    └── style.css    # CSS styles
```

## Health Check System

The controller implements a health check system that:
- Periodically checks node health
- Simulates random node failures
- Handles leader election when nodes fail
- Manages partition leadership changes

## Partition Management

The controller handles:
- Partition creation and deletion
- Partition distribution across nodes
- Leader election for partitions
- Replica management
- Partition transfer between nodes 