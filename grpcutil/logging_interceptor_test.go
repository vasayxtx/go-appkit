package grpcutil

import (
	"context"
	"strings"
	"testing"

	"github.com/acronis/go-appkit/log"
	"github.com/acronis/go-appkit/log/logtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestLoggingServerUnaryInterceptor(t *testing.T) {
	const headerRequestID = "test-request-id"
	const headerUserAgent = "test-user-agent"

	logger := logtest.NewRecorder()

	svc, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(RequestIDServerUnaryInterceptor(), LoggingServerUnaryInterceptor(logger))},
		[]grpc.DialOption{grpc.WithUserAgent(headerUserAgent)})
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	requireCommonFields := func(t *testing.T, logEntry logtest.RecordedEntry) {
		requireLogFieldString(t, logEntry, "request_id", headerRequestID)
		require.NotEmpty(t, getLogFieldAsString(logEntry, "int_request_id"))
		requireLogFieldString(t, logEntry, "grpc_service", "grpc.testing.TestService")
		requireLogFieldString(t, logEntry, "grpc_method", "UnaryCall")
		requireLogFieldString(t, logEntry, "grpc_method_type", methodTypeUnary)
		require.True(t, strings.HasPrefix(getLogFieldAsString(logEntry, "remote_addr"), "127.0.0.1:"))
		requireLogFieldString(t, logEntry, "user_agent", headerUserAgent+" grpc-go/"+grpc.Version)
	}

	permissionDeniedErr := status.Error(codes.PermissionDenied, "Permission denied")

	tests := []struct {
		name             string
		md               metadata.MD
		unaryCallHandler func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error)
		wantErr          error
		wantCode         codes.Code
	}{
		{
			name:     "log gRPC call is started and finished",
			md:       metadata.Pairs(headerRequestIDKey, headerRequestID),
			wantCode: codes.OK,
		},
		{
			name: "log gRPC call is started and finished with error",
			md:   metadata.Pairs(headerRequestIDKey, headerRequestID),
			unaryCallHandler: func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
				return nil, permissionDeniedErr
			},
			wantErr:  permissionDeniedErr,
			wantCode: codes.PermissionDenied,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.unaryCallHandler != nil {
				svc.SwitchUnaryCallHandler(tt.unaryCallHandler)
			}

			var headers metadata.MD
			reqCtx := metadata.NewOutgoingContext(context.Background(), tt.md)
			resp, respErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{}, grpc.Header(&headers))
			if tt.wantErr != nil {
				require.ErrorIs(t, respErr, tt.wantErr)
			} else {
				require.NoError(t, respErr)
				require.Equal(t, "test", string(resp.Payload.GetBody()))
			}

			require.Equal(t, 2, len(logger.Entries()))

			callStartedLogEntry := logger.Entries()[0]
			require.Contains(t, callStartedLogEntry.Text, "gRPC call started")
			require.Equal(t, log.LevelInfo, callStartedLogEntry.Level)
			requireCommonFields(t, callStartedLogEntry)

			callFinishedLogEntry := logger.Entries()[1]
			require.Contains(t, callFinishedLogEntry.Text, "gRPC call finished")
			require.Equal(t, log.LevelInfo, callFinishedLogEntry.Level)
			requireCommonFields(t, callFinishedLogEntry)

			requireLogFieldString(t, callFinishedLogEntry, "grpc_code", tt.wantCode.String())
			_, found := callFinishedLogEntry.FindField("duration_ms")
			require.True(t, found)

			if tt.wantErr != nil {
				requireLogFieldString(t, callFinishedLogEntry, "grpc_error", tt.wantErr.Error())
			}

			logger.Reset()
			svc.Reset()
		})
	}
}

func requireLogFieldString(t *testing.T, logEntry logtest.RecordedEntry, key, want string) {
	t.Helper()
	logField, found := logEntry.FindField(key)
	require.True(t, found)
	require.Equal(t, want, string(logField.Bytes))
}

func getLogFieldAsString(logEntry logtest.RecordedEntry, key string) string {
	logField, found := logEntry.FindField(key)
	if !found {
		return ""
	}
	return string(logField.Bytes)
}
