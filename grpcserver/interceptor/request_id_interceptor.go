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

// RequestIDServerUnaryInterceptor is a gRPC unary interceptor that extracts the request ID from the incoming context metadata
// and attaches it to the context. If the request ID is missing, a new one is generated.
func RequestIDServerUnaryInterceptor() func(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var requestID string
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			requestIDList := md.Get(headerRequestIDKey)
			if len(requestIDList) > 0 {
				requestID = requestIDList[0]
			}
		}
		if requestID == "" {
			requestID = xid.New().String()
		}
		ctx = NewContextWithRequestID(ctx, requestID)
		if err := grpc.SetHeader(ctx, metadata.Pairs(headerRequestIDKey, requestID)); err != nil {
			return nil, err
		}

		internalRequestID := xid.New().String()
		ctx = NewContextWithInternalRequestID(ctx, internalRequestID)
		if err := grpc.SetHeader(ctx, metadata.Pairs(headerRequestInternalIDKey, internalRequestID)); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}
