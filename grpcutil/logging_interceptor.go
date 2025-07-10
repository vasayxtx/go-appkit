package grpcutil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/acronis/go-appkit/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const headerUserAgentKey = "user-agent"

const methodTypeUnary = "unary"

// LoggingServerUnaryInterceptor is a gRPC unary interceptor that logs the start and end of each RPC call.
func LoggingServerUnaryInterceptor(logger log.FieldLogger) func(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startTime := GetRequestStartTimeFromContext(ctx)
		if startTime.IsZero() {
			startTime = time.Now()
			ctx = NewContextWithRequestStartTime(ctx, startTime)
		}

		service, method := splitFullMethodName(info.FullMethod)
		var remoteAddr string
		if p, ok := peer.FromContext(ctx); ok {
			remoteAddr = p.Addr.String()
		}
		var userAgent string
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if userAgentList := md.Get(headerUserAgentKey); len(userAgentList) > 0 {
				userAgent = userAgentList[0]
			}
		}

		l := logger.With(
			log.String("request_id", GetRequestIDFromContext(ctx)),
			log.String("int_request_id", GetInternalRequestIDFromContext(ctx)),
			log.String("grpc_service", service),
			log.String("grpc_method", method),
			log.String("grpc_method_type", methodTypeUnary),
			log.String("remote_addr", remoteAddr),
			log.String("user_agent", userAgent),
		)
		l.Info("gRPC call started")

		resp, err := handler(NewContextWithLogger(ctx, l), req)
		duration := time.Since(startTime)

		logFields := make([]log.Field, 0, 4)
		logFields = append(
			logFields,
			log.String("grpc_code", status.Code(err).String()),
			log.Int64("duration_ms", duration.Milliseconds()),
		)
		if err != nil {
			logFields = append(logFields, log.String("grpc_error", err.Error()))
		}
		l.Info(fmt.Sprintf("gRPC call finished in %.3fs", duration.Seconds()), logFields...)

		return resp, err
	}
}

func splitFullMethodName(fullMethod string) (string, string) {
	fullMethod = strings.TrimPrefix(fullMethod, "/") // remove leading slash
	if i := strings.Index(fullMethod, "/"); i >= 0 {
		return fullMethod[:i], fullMethod[i+1:]
	}
	return "unknown", "unknown"
}
