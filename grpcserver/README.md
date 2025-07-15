# grpcserver

A gRPC server implementation with YAML/JSON based configuration and built-in interceptors for logging, metrics, recovery, and request ID handling.

## Features

- **YAML/JSON Configuration**: Easily configurable server settings
- **Built-in Interceptors**: Pre-configured interceptors for common functionality
- **Graceful Shutdown**: Supports graceful shutdown with configurable timeout
- **Metrics Integration**: Prometheus metrics collection

## Usage

### Basic Server Setup

```go
package main

import (
    "github.com/acronis/go-appkit/grpcserver"
    "github.com/acronis/go-appkit/log"
)

func main() {
    // Create configuration
    cfg := grpcserver.NewDefaultConfig()
    cfg.Address = ":50051"
    
    // Create logger
    logger := log.NewDiscard()
    
    // Create server
    server, err := grpcserver.New(cfg, logger)
    if err != nil {
        panic(err)
    }
    
    // Register your gRPC services here
    // pb.RegisterYourServiceServer(server.GRPCServer, &yourServiceImpl{})
    
    // Start server
    fatalError := make(chan error, 1)
    go server.Start(fatalError)
    
    // Handle shutdown...
}
```

### Configuration Options

The server supports extensive configuration through the `Config` struct:

```go
cfg := grpcserver.NewDefaultConfig()

// Network configuration
cfg.Address = ":8080"
cfg.UnixSocketPath = "/tmp/grpc.sock" // Alternative to TCP

// TLS configuration
cfg.TLS.Enabled = true
cfg.TLS.Certificate = "/path/to/cert.pem"
cfg.TLS.Key = "/path/to/key.pem"

// Keepalive settings
cfg.Keepalive.Time = config.TimeDuration(2 * time.Minute)
cfg.Keepalive.Timeout = config.TimeDuration(20 * time.Second)

// Message size limits
cfg.Limits.MaxRecvMessageSize = config.ByteSize(4 * 1024 * 1024) // 4MB
cfg.Limits.MaxSendMessageSize = config.ByteSize(4 * 1024 * 1024) // 4MB
cfg.Limits.MaxConcurrentStreams = 100

// Logging configuration
cfg.Log.CallStart = true
cfg.Log.SlowCallThreshold = config.TimeDuration(time.Second)
cfg.Log.ExcludedMethods = []string{"/health/check"}

// Shutdown timeout
cfg.Timeouts.Shutdown = config.TimeDuration(5 * time.Second)
```

### Custom Options

You can customize the server behavior using functional options:

```go
// Custom interceptors
server, err := grpcserver.New(cfg, logger,
    grpcserver.WithUnaryInterceptors(yourUnaryInterceptor),
    grpcserver.WithStreamInterceptors(yourStreamInterceptor),
    grpcserver.WithLoggingOptions(grpcserver.LoggingOptions{
        UnaryCustomLoggerProvider: func(ctx context.Context, info *grpc.UnaryServerInfo) log.FieldLogger {
            return logger.With(log.String("custom", "value"))
        },
    }),
    grpcserver.WithMetricsOptions(grpcserver.MetricsOptions{
        Namespace: "myapp",
        DurationBuckets: []float64{0.1, 0.5, 1.0, 5.0},
    }),
)
```

### Metrics

The server automatically collects Prometheus metrics:

```go
// Register metrics
server.MustRegisterMetrics()

// Metrics will be available:
// - grpc_call_duration_seconds (histogram)
// - grpc_calls_in_flight (gauge)
```

## Built-in Interceptors

The server comes with pre-configured interceptors that provide essential functionality. For detailed information about individual interceptors, see the [interceptor package documentation](interceptor/README.md).

### Interceptor Chain

The server automatically configures the following interceptor chain:

1. **Call Start Time**: Records the start time for duration measurement
2. **Request ID**: Generates and manages request IDs
3. **Logging**: Logs call information and performance metrics
4. **Recovery**: Recovers from panics and returns proper gRPC errors
5. **Metrics**: Collects Prometheus metrics

## Graceful Shutdown

The server supports graceful shutdown:

```go
// Graceful shutdown with timeout
err := server.Stop(true)

// Immediate shutdown
err := server.Stop(false)
```

## Service Integration

The server implements the `service.Unit` interface for easy integration with service managers:

```go
import "github.com/acronis/go-appkit/service"

// The server can be used with service managers
var unit service.Unit = server

// Start in service context
fatalError := make(chan error)
go unit.Start(fatalError)

// Stop gracefully
err := unit.Stop(true)
```

## Error Handling

The server provides proper error handling and logging:

- Panics are recovered and logged with stack traces
- gRPC errors are properly formatted
- Slow calls are automatically detected and logged
- Connection errors are handled gracefully