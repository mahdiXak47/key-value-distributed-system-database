# Key-Value Store Load Balancer

A load balancer for the distributed key-value store system that distributes requests across multiple nodes and ensures high availability.

## Features

- Round-robin load balancing
- Health check monitoring
- Dynamic node management
- Request forwarding
- Error handling
- Automatic failover

## Architecture

The load balancer consists of:
- Node management system
- Health check monitoring
- Request forwarding mechanism
- Round-robin selection algorithm

## Technical Details

- Built with Go
- Uses HTTP/JSON for communication
- Implements health check system
- Handles request forwarding
- Manages node availability
- Provides node management endpoints

## Running the Application

1. Make sure you have Go installed
2. Navigate to the load-balancer directory:
   ```bash
   cd load-balancer
   ```
3. Run the application:
   ```bash
   go run main.go
   ```
4. The load balancer will start on port 8081

## Configuration

The load balancer:
- Runs on port 8081 by default
- Performs health checks every 5 seconds
- Starts with three default nodes (configurable)

## API Endpoints

### Key-Value Operations
- `POST /set` - Set a key-value pair
- `GET /get` - Get a value by key
- `DELETE /delete` - Delete a key-value pair

### Node Management
- `POST /node/add` - Add a new node
  - Parameter: `address` - Node address
- `POST /node/remove` - Remove a node
  - Parameter: `address` - Node address

## Health Check System

The load balancer implements a health check system that:
- Periodically checks node health
- Marks nodes as inactive if they fail health checks
- Automatically skips inactive nodes in the round-robin selection
- Maintains node status information

## Load Balancing Strategy

The load balancer uses a round-robin strategy:
- Distributes requests evenly across active nodes
- Skips inactive nodes automatically
- Maintains request order within each node
- Handles node failures gracefully

## Directory Structure

```
load-balancer/
└── main.go    # Main application code
``` 