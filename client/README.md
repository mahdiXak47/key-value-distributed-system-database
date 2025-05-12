# Key-Value Store Client

A modern web-based client application for interacting with the distributed key-value store system.

## Features

- Clean, modern user interface
- Support for basic key-value operations:
  - Set: Store a key-value pair
  - Get: Retrieve a value by key
  - Delete: Remove a key-value pair
- Automatic retry mechanism for failed requests
- Real-time operation status feedback
- Responsive design for all devices

## Architecture

The client application consists of:
- HTTP client with retry mechanism
- Web interface for user interaction
- JSON request/response handling
- Error handling and response formatting

## Technical Details

- Built with Go and modern web technologies
- Uses HTTP/JSON for communication
- Implements retry logic (3 retries with 2-second delay)
- Serves a responsive web interface

## Running the Application

1. Make sure you have Go installed
2. Navigate to the client directory:
   ```bash
   cd client
   ```
3. Run the application:
   ```bash
   go run main.go
   ```
4. Access the web interface at `http://localhost:8082`

## Configuration

The client is configured to connect to the load balancer at `http://localhost:8081`. You can modify this by changing the `baseURL` constant in `main.go`.

## Directory Structure

```
client/
├── main.go           # Main application code
├── templates/        # HTML templates
│   └── index.html    # Main interface template
└── static/          # Static assets
    └── style.css    # CSS styles
``` 