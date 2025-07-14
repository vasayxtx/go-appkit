/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/acronis/go-appkit/log"
)

const (
	headerUserAgentKey = "user-agent"
	methodTypeUnary    = "unary"
	methodTypeStream   = "stream"
)

const defaultSlowCallThreshold = 1 * time.Second

// CustomLoggerProvider returns a custom logger or nil based on the gRPC context and method info.
type CustomLoggerProvider func(ctx context.Context, info *grpc.UnaryServerInfo) log.FieldLogger

// CustomStreamLoggerProvider returns a custom logger or nil based on the gRPC context and stream method info.
type CustomStreamLoggerProvider func(ctx context.Context, info *grpc.StreamServerInfo) log.FieldLogger

// LoggingOption represents a configuration option for the logging interceptor.
type LoggingOption func(*loggingOptions)

type loggingOptions struct {
	callStart                  bool
	callHeaders                map[string]string
	excludedMethods            []string
	addCallInfoToLogger        bool
	slowCallThreshold          time.Duration
	customLoggerProvider       CustomLoggerProvider
	customStreamLoggerProvider CustomStreamLoggerProvider
}

// WithLoggingCallStart enables logging of call start events.
func WithLoggingCallStart(logCallStart bool) LoggingOption {
	return func(opts *loggingOptions) {
		opts.callStart = logCallStart
	}
}

// WithLoggingCallHeaders specifies custom headers to log from gRPC metadata.
func WithLoggingCallHeaders(headers map[string]string) LoggingOption {
	return func(opts *loggingOptions) {
		opts.callHeaders = headers
	}
}

// WithLoggingExcludedMethods specifies gRPC methods to exclude from logging.
func WithLoggingExcludedMethods(methods ...string) LoggingOption {
	return func(opts *loggingOptions) {
		opts.excludedMethods = methods
	}
}

// WithLoggingAddCallInfoToLogger adds call information to the logger context.
func WithLoggingAddCallInfoToLogger(addCallInfo bool) LoggingOption {
	return func(opts *loggingOptions) {
		opts.addCallInfoToLogger = addCallInfo
	}
}

// WithLoggingSlowCallThreshold sets the threshold for slow call detection.
func WithLoggingSlowCallThreshold(threshold time.Duration) LoggingOption {
	return func(opts *loggingOptions) {
		opts.slowCallThreshold = threshold
	}
}

// WithLoggingCustomLoggerProvider sets a custom logger provider function.
func WithLoggingCustomLoggerProvider(provider CustomLoggerProvider) LoggingOption {
	return func(opts *loggingOptions) {
		opts.customLoggerProvider = provider
	}
}

// WithLoggingCustomStreamLoggerProvider sets a custom logger provider function for stream interceptors.
func WithLoggingCustomStreamLoggerProvider(provider CustomStreamLoggerProvider) LoggingOption {
	return func(opts *loggingOptions) {
		opts.customStreamLoggerProvider = provider
	}
}

// LoggingServerUnaryInterceptor is a gRPC unary interceptor that logs the start and end of each RPC call.
func LoggingServerUnaryInterceptor(logger log.FieldLogger, options ...LoggingOption) func(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	opts := &loggingOptions{
		slowCallThreshold: defaultSlowCallThreshold,
	}
	for _, option := range options {
		option(opts)
	}
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		startTime := GetCallStartTimeFromContext(ctx)
		if startTime.IsZero() {
			startTime = time.Now()
			ctx = NewContextWithCallStartTime(ctx, startTime)
		}

		loggerForNext := logger
		if opts.customLoggerProvider != nil {
			if l := opts.customLoggerProvider(ctx, info); l != nil {
				loggerForNext = l
			}
		}
		loggerForNext = loggerForNext.With(
			log.String("request_id", GetRequestIDFromContext(ctx)),
			log.String("int_request_id", GetInternalRequestIDFromContext(ctx)),
			log.String("trace_id", GetTraceIDFromContext(ctx)),
		)

		logFields := buildCommonLogFields(ctx, info.FullMethod, methodTypeUnary, opts)

		logger = loggerForNext.With(logFields...)
		if opts.addCallInfoToLogger {
			loggerForNext = logger
		}

		noLog := isLoggingDisabled(info.FullMethod, opts.excludedMethods)

		if opts.callStart && !noLog {
			logger.Info("gRPC call started")
		}

		lp := &LoggingParams{}
		ctx = NewContextWithLoggingParams(NewContextWithLogger(ctx, loggerForNext), lp)

		resp, err := handler(ctx, req)
		duration := time.Since(startTime)

		logCallCompletion(logger, logFields, lp, duration, err, opts, info.FullMethod)

		return resp, err
	}
}

// LoggingServerStreamInterceptor is a gRPC stream interceptor that logs the start and end of each RPC call.
func LoggingServerStreamInterceptor(logger log.FieldLogger, options ...LoggingOption) func(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	opts := &loggingOptions{
		slowCallThreshold: defaultSlowCallThreshold,
	}
	for _, option := range options {
		option(opts)
	}
	return func(
		srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()
		startTime := GetCallStartTimeFromContext(ctx)
		if startTime.IsZero() {
			startTime = time.Now()
			ctx = NewContextWithCallStartTime(ctx, startTime)
		}

		loggerForNext := logger
		if opts.customStreamLoggerProvider != nil {
			if l := opts.customStreamLoggerProvider(ctx, info); l != nil {
				loggerForNext = l
			}
		}
		loggerForNext = loggerForNext.With(
			log.String("request_id", GetRequestIDFromContext(ctx)),
			log.String("int_request_id", GetInternalRequestIDFromContext(ctx)),
			log.String("trace_id", GetTraceIDFromContext(ctx)),
		)

		logFields := buildCommonLogFields(ctx, info.FullMethod, methodTypeStream, opts)

		logger = loggerForNext.With(logFields...)
		if opts.addCallInfoToLogger {
			loggerForNext = logger
		}

		noLog := isLoggingDisabled(info.FullMethod, opts.excludedMethods)

		if opts.callStart && !noLog {
			logger.Info("gRPC call started")
		}

		lp := &LoggingParams{}
		ctx = NewContextWithLoggingParams(NewContextWithLogger(ctx, loggerForNext), lp)

		// Create a wrapped stream with the updated context
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		err := handler(srv, wrappedStream)
		duration := time.Since(startTime)

		logCallCompletion(logger, logFields, lp, duration, err, opts, info.FullMethod)

		return err
	}
}

// buildCommonLogFields builds the common log fields for both unary and stream interceptors
func buildCommonLogFields(ctx context.Context, fullMethod, methodType string, opts *loggingOptions) []log.Field {
	service, method := splitFullMethodName(fullMethod)
	var remoteAddr string
	var remoteAddrIP string
	var remoteAddrPort uint16
	if p, ok := peer.FromContext(ctx); ok {
		remoteAddr = p.Addr.String()
		if addrIP, addrPort, err := net.SplitHostPort(remoteAddr); err == nil {
			remoteAddrIP = addrIP
			if port, pErr := strconv.ParseUint(addrPort, 10, 16); pErr == nil {
				remoteAddrPort = uint16(port)
			}
		}
	}

	var userAgent string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if userAgentList := md.Get(headerUserAgentKey); len(userAgentList) > 0 {
			userAgent = userAgentList[0]
		}
	}

	logFields := make([]log.Field, 0, 8)
	logFields = append(
		logFields,
		log.String("grpc_service", service),
		log.String("grpc_method", method),
		log.String("grpc_method_type", methodType),
		log.String("remote_addr", remoteAddr),
		log.String("user_agent", userAgent),
	)

	if remoteAddrIP != "" {
		logFields = append(logFields, log.String("remote_addr_ip", remoteAddrIP))
		if remoteAddrPort != 0 {
			logFields = append(logFields, log.Uint16("remote_addr_port", remoteAddrPort))
		}
	}

	if len(opts.callHeaders) > 0 {
		// Add custom headers from metadata
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			for headerName, logKey := range opts.callHeaders {
				if headerValues := md.Get(headerName); len(headerValues) > 0 {
					logFields = append(logFields, log.String(logKey, headerValues[0]))
				}
			}
		}
	}

	return logFields
}

// logCallCompletion logs the completion of a gRPC call with timing and error information
func logCallCompletion(
	logger log.FieldLogger,
	logFields []log.Field,
	lp *LoggingParams,
	duration time.Duration,
	err error,
	opts *loggingOptions,
	fullMethod string,
) {
	grpcCode := status.Code(err)
	noLog := isLoggingDisabled(fullMethod, opts.excludedMethods)

	if !noLog || grpcCode != codes.OK { // Log if not excluded or if there's an error
		if duration >= opts.slowCallThreshold {
			lp.fields = append(
				lp.fields,
				log.Bool("slow_request", true),
				log.Object("time_slots", lp.getTimeSlots()),
			)
		}
		logFields = append(
			logFields,
			log.String("grpc_code", grpcCode.String()),
			log.Int64("duration_ms", duration.Milliseconds()),
		)
		if err != nil {
			logFields = append(logFields, log.String("grpc_error", err.Error()))
		}
		logger.Info(fmt.Sprintf("gRPC call finished in %.3fs", duration.Seconds()), append(logFields, lp.fields...)...)
	}
}

func splitFullMethodName(fullMethod string) (string, string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/") // remove leading slash
	if i := strings.Index(fullMethod, "/"); i >= 0 {
		return fullMethod[:i], fullMethod[i+1:]
	}
	return "unknown", "unknown"
}

func isLoggingDisabled(fullMethod string, excludedMethods []string) bool {
	for _, method := range excludedMethods {
		if fullMethod == method {
			return true
		}
	}
	return false
}
