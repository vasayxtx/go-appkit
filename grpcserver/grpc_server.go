/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package grpcserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/acronis/go-appkit/grpcserver/interceptor"
	"github.com/acronis/go-appkit/log"
	"github.com/acronis/go-appkit/service"
)

// GRPCCallMetricsOptions represents options for gRPC request metrics that used in GRPCServer.
type GRPCCallMetricsOptions struct {
	Namespace       string
	DurationBuckets []float64
	ConstLabels     prometheus.Labels
}

// Option represents a functional option for configuring GRPCServer.
type Option func(*serverOptions)

// serverOptions holds all the configuration options for the server.
type serverOptions struct {
	unaryInterceptors  []grpc.UnaryServerInterceptor
	streamInterceptors []grpc.StreamServerInterceptor
	grpcRequestMetrics GRPCCallMetricsOptions
}

// WithUnaryInterceptors adds unary interceptors to the server.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) Option {
	return func(o *serverOptions) {
		o.unaryInterceptors = append(o.unaryInterceptors, interceptors...)
	}
}

// WithStreamInterceptors adds stream interceptors to the server.
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) Option {
	return func(o *serverOptions) {
		o.streamInterceptors = append(o.streamInterceptors, interceptors...)
	}
}

// WithGRPCCallMetricsOptions configures gRPC request metrics.
func WithGRPCCallMetricsOptions(opts GRPCCallMetricsOptions) Option {
	return func(o *serverOptions) {
		o.grpcRequestMetrics = opts
	}
}

// GRPCServer represents a wrapper around grpc.Server with additional fields and methods.
// It also implements service.Unit and service.MetricsRegisterer interfaces.
type GRPCServer struct {
	GRPCServer *grpc.Server
	Logger     log.FieldLogger

	address                  atomic.Value
	unixSocketPath           string
	shutdownTimeout          time.Duration
	grpcServerDone           chan struct{}
	grpcReqPrometheusMetrics *interceptor.PrometheusMetrics
}

var _ service.Unit = (*GRPCServer)(nil)
var _ service.MetricsRegisterer = (*GRPCServer)(nil)

// New creates a new GRPCServer with predefined logging, metrics collecting,
// recovering after panics and request ID functionality.
func New(cfg *Config, logger log.FieldLogger, options ...Option) (*GRPCServer, error) {
	// Apply options
	opts := &serverOptions{}
	for _, opt := range options {
		opt(opts)
	}

	var serverOpts []grpc.ServerOption

	// Add TLS credentials if enabled
	if cfg.TLS.Enabled {
		cert, err := tls.LoadX509KeyPair(cfg.TLS.Certificate, cfg.TLS.Key)
		if err != nil {
			return nil, fmt.Errorf("load TLS certificates: %w", err)
		}
		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
		})
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	// Add keepalive parameters
	serverOpts = append(serverOpts, grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    time.Duration(cfg.Keepalive.Time),
		Timeout: time.Duration(cfg.Keepalive.Timeout),
	}))
	serverOpts = append(serverOpts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             time.Duration(cfg.Keepalive.MinTime),
		PermitWithoutStream: true,
	}))

	// Add limits
	if cfg.Limits.MaxConcurrentStreams > 0 {
		serverOpts = append(serverOpts, grpc.MaxConcurrentStreams(cfg.Limits.MaxConcurrentStreams))
	}
	if cfg.Limits.MaxRecvMessageSize > 0 {
		maxRecvMsgSize := int(cfg.Limits.MaxRecvMessageSize)
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(maxRecvMsgSize))
	}
	if cfg.Limits.MaxSendMessageSize > 0 {
		maxSendMsgSize := int(cfg.Limits.MaxSendMessageSize)
		serverOpts = append(serverOpts, grpc.MaxSendMsgSize(maxSendMsgSize))
	}

	promMetrics := interceptor.NewPrometheusMetrics(
		interceptor.WithPrometheusNamespace(opts.grpcRequestMetrics.Namespace),
		interceptor.WithPrometheusDurationBuckets(opts.grpcRequestMetrics.DurationBuckets),
		interceptor.WithPrometheusConstLabels(opts.grpcRequestMetrics.ConstLabels))

	loggingOptions := []interceptor.LoggingOption{
		interceptor.WithLoggingCallStart(cfg.Log.CallStart),
		interceptor.WithLoggingSlowCallThreshold(time.Duration(cfg.Log.SlowCallThreshold)),
		interceptor.WithLoggingExcludedMethods(cfg.Log.ExcludedMethods...),
	}

	// Build unary interceptors chain
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		callStartTimeUnaryInterceptor(),
		interceptor.RequestIDServerUnaryInterceptor(),
		interceptor.LoggingServerUnaryInterceptor(logger, loggingOptions...),
		interceptor.RecoveryServerUnaryInterceptor(),
		interceptor.MetricsServerUnaryInterceptor(promMetrics),
	}
	unaryInterceptors = append(unaryInterceptors, opts.unaryInterceptors...)
	if len(unaryInterceptors) > 0 {
		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}

	// Build stream interceptors chain
	streamInterceptors := []grpc.StreamServerInterceptor{
		callStartTimeStreamInterceptor(),
		interceptor.RequestIDServerStreamInterceptor(),
		interceptor.LoggingServerStreamInterceptor(logger, loggingOptions...),
		interceptor.RecoveryServerStreamInterceptor(),
		interceptor.MetricsServerStreamInterceptor(promMetrics),
	}
	streamInterceptors = append(streamInterceptors, opts.streamInterceptors...)
	if len(opts.streamInterceptors) > 0 {
		serverOpts = append(serverOpts, grpc.ChainStreamInterceptor(streamInterceptors...))
	}

	grpcServer := &GRPCServer{
		GRPCServer:               grpc.NewServer(serverOpts...),
		Logger:                   logger,
		unixSocketPath:           cfg.UnixSocketPath,
		shutdownTimeout:          time.Duration(cfg.Timeouts.Shutdown),
		grpcReqPrometheusMetrics: promMetrics,
	}
	if cfg.UnixSocketPath != "" {
		grpcServer.address.Store(cfg.UnixSocketPath)
	} else {
		grpcServer.address.Store(cfg.Address)
	}
	return grpcServer, nil
}

// Start starts the gRPC server in a blocking way.
// It's supposed that this method will be called in a separate goroutine.
// If a fatal error occurs, it will be sent to the fatalError channel.
func (s *GRPCServer) Start(fatalError chan<- error) {
	s.grpcServerDone = make(chan struct{})
	defer close(s.grpcServerDone)

	logger := s.Logger.With(log.String("address", s.Address()))

	network := "tcp"
	if s.unixSocketPath != "" {
		network = "unix"
		if err := os.Remove(s.unixSocketPath); err != nil && !os.IsNotExist(err) {
			fatalError <- fmt.Errorf("remove unix socket file %q: %w", s.unixSocketPath, err)
			return
		}
	}

	logger.Info("starting gRPC server...")

	var err error
	var listener net.Listener
	if listener, err = net.Listen(network, s.Address()); err != nil {
		logger.Error("gRPC server listen error", log.Error(err))
		fatalError <- err
		return
	}

	s.address.Store(listener.Addr().String())

	if err = s.GRPCServer.Serve(listener); err != nil {
		logger.Error("gRPC server error", log.Error(err))
		fatalError <- err
		return
	}
}

func (s *GRPCServer) Stop(gracefully bool) error {
	if !gracefully {
		s.Logger.Info("stopping gRPC server...")
		s.GRPCServer.Stop()
		if s.grpcServerDone != nil {
			<-s.grpcServerDone // wait for the server to be stopped
		}
		return nil
	}

	s.Logger.Info("stopping gRPC server gracefully...", log.Duration("timeout", s.shutdownTimeout))

	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		s.GRPCServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		s.Logger.Info("gRPC server gracefully stopped")
	case <-ctx.Done():
		s.Logger.Info("gRPC server graceful stop timed out, stopping forcefully...")
		s.GRPCServer.Stop()
	}

	if s.grpcServerDone != nil {
		<-s.grpcServerDone // wait for the server to be stopped
	}

	return nil
}

// MustRegisterMetrics registers metrics in Prometheus client and panics if any error occurs.
func (s *GRPCServer) MustRegisterMetrics() {
	if s.grpcReqPrometheusMetrics != nil {
		s.grpcReqPrometheusMetrics.MustRegister()
	}
}

// UnregisterMetrics unregisters metrics in Prometheus client.
func (s *GRPCServer) UnregisterMetrics() {
	if s.grpcReqPrometheusMetrics != nil {
		s.grpcReqPrometheusMetrics.Unregister()
	}
}

func (s *GRPCServer) Address() string {
	address, _ := s.address.Load().(string)
	return address
}

func callStartTimeUnaryInterceptor() func(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		return handler(interceptor.NewContextWithCallStartTime(ctx, time.Now()), req)
	}
}

func callStartTimeStreamInterceptor() func(
	srv interface{},
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	return func(
		srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		wrappedStream := &interceptor.WrappedServerStream{
			ServerStream: ss,
			Ctx:          interceptor.NewContextWithCallStartTime(ss.Context(), time.Now()),
		}
		return handler(srv, wrappedStream)
	}
}
