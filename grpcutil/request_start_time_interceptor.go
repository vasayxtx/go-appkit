package grpcutil

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

func RequestStartTimeServerUnaryInterceptor() func(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		return handler(NewContextWithRequestStartTime(ctx, time.Now()), req)
	}
}
