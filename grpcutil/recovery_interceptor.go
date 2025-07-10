package grpcutil

import (
	"context"
	"fmt"
	"runtime"

	"github.com/acronis/go-appkit/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const recoveryStackSize = 8192

var InternalError = status.Error(codes.Internal, "Internal error")

// RecoveryServerUnaryInterceptor is a gRPC unary interceptor that recovers from panics and returns Internal error.
func RecoveryServerUnaryInterceptor() func(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		defer func() {
			if p := recover(); p != nil {
				if logger := GetLoggerFromContext(ctx); logger != nil {
					stack := make([]byte, recoveryStackSize)
					stack = stack[:runtime.Stack(stack, false)]
					logger.Error(fmt.Sprintf("Panic: %+v", p), log.Bytes("stack", stack))
				}
				err = InternalError
			}
		}()
		return handler(ctx, req)
	}
}
