/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"

	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	headerRequestIDKey         = "x-request-id"
	headerRequestInternalIDKey = "x-int-request-id"
)

// requestIDOptions represents options for RequestIDServerUnaryInterceptor.
type requestIDOptions struct {
	GenerateID         func() string
	GenerateInternalID func() string
}

// RequestIDOption is a function type for configuring requestIDOptions.
type RequestIDOption func(*requestIDOptions)

func newID() string {
	return xid.New().String()
}

// WithRequestIDGenerator sets the function for generating request IDs.
func WithRequestIDGenerator(generator func() string) RequestIDOption {
	return func(opts *requestIDOptions) {
		opts.GenerateID = generator
	}
}

// WithInternalRequestIDGenerator sets the function for generating internal request IDs.
func WithInternalRequestIDGenerator(generator func() string) RequestIDOption {
	return func(opts *requestIDOptions) {
		opts.GenerateInternalID = generator
	}
}

// RequestIDServerUnaryInterceptor is a gRPC unary interceptor that extracts the request ID from the incoming context metadata
// and attaches it to the context. If the request ID is missing, a new one is generated.
func RequestIDServerUnaryInterceptor(options ...RequestIDOption) func(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	opts := requestIDOptions{
		GenerateID:         newID,
		GenerateInternalID: newID,
	}
	for _, option := range options {
		option(&opts)
	}

	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx = processRequestIDs(ctx, &opts)
		return handler(ctx, req)
	}
}

// RequestIDServerStreamInterceptor is a gRPC stream interceptor that extracts the request ID from the incoming context metadata
// and attaches it to the context. If the request ID is missing, a new one is generated.
// This interceptor allows tracing the whole gRPC call itself and all individual stream request messages.
func RequestIDServerStreamInterceptor(options ...RequestIDOption) func(
	srv interface{},
	ss grpc.ServerStream,
	_ *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	opts := requestIDOptions{
		GenerateID:         newID,
		GenerateInternalID: newID,
	}
	for _, option := range options {
		option(&opts)
	}

	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		var requestID string
		if md, ok := metadata.FromIncomingContext(ss.Context()); ok {
			requestIDList := md.Get(headerRequestIDKey)
			if len(requestIDList) > 0 {
				requestID = requestIDList[0]
			}
		}
		if requestID == "" {
			requestID = opts.GenerateID()
		}

		internalRequestID := opts.GenerateInternalID()

		// Set headers for streaming calls
		headerMD := metadata.Pairs(
			headerRequestIDKey, requestID,
			headerRequestInternalIDKey, internalRequestID,
		)
		if err := ss.SetHeader(headerMD); err != nil {
			return err
		}

		// Create context with request IDs
		ctx := NewContextWithRequestID(ss.Context(), requestID)
		ctx = NewContextWithInternalRequestID(ctx, internalRequestID)

		wrappedStream := &requestIDServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}
		return handler(srv, wrappedStream)
	}
}

// requestIDServerStream wraps grpc.ServerStream to provide a custom context with request IDs
type requestIDServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the custom context with request IDs
func (s *requestIDServerStream) Context() context.Context {
	return s.ctx
}

// processRequestIDs extracts or generates request IDs and adds them to the context and headers
func processRequestIDs(ctx context.Context, opts *requestIDOptions) context.Context {
	var requestID string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		requestIDList := md.Get(headerRequestIDKey)
		if len(requestIDList) > 0 {
			requestID = requestIDList[0]
		}
	}
	if requestID == "" {
		requestID = opts.GenerateID()
	}
	ctx = NewContextWithRequestID(ctx, requestID)
	if err := grpc.SetHeader(ctx, metadata.Pairs(headerRequestIDKey, requestID)); err != nil {
		// Note: In stream interceptors, we can't return the error directly, but we still set the header
		// The error will be handled by the gRPC framework
	}

	internalRequestID := opts.GenerateInternalID()
	ctx = NewContextWithInternalRequestID(ctx, internalRequestID)
	if err := grpc.SetHeader(ctx, metadata.Pairs(headerRequestInternalIDKey, internalRequestID)); err != nil {
		// Note: In stream interceptors, we can't return the error directly, but we still set the header
		// The error will be handled by the gRPC framework
	}

	return ctx
}
