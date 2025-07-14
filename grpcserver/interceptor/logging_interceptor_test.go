/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"strings"
	"testing"
	"time"

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
		requireLogFieldString(t, logEntry, "grpc_method_type", string(CallMethodTypeUnary))
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

			require.Equal(t, 1, len(logger.Entries()))

			callFinishedLogEntry := logger.Entries()[0]
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

func TestLoggingServerUnaryInterceptorWithOptions(t *testing.T) {
	const headerRequestID = "test-request-id"
	const headerUserAgent = "test-user-agent"

	logger := logtest.NewRecorder()

	// Test with request start enabled
	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger, WithLoggingCallStart(true)),
		)},
		[]grpc.DialOption{grpc.WithUserAgent(headerUserAgent)})
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	// Should have 2 entries: started and finished
	require.Equal(t, 2, len(logger.Entries()))
	require.Contains(t, logger.Entries()[0].Text, "gRPC call started")
	require.Contains(t, logger.Entries()[1].Text, "gRPC call finished")
}

func TestLoggingServerUnaryInterceptor_AllOptions(t *testing.T) {
	const (
		headerRequestID   = "test-request-id"
		headerUserAgent   = "test-user-agent"
		headerTraceID     = "test-trace-id"
		headerCustomValue = "custom-header-value"
	)

	logger := logtest.NewRecorder()
	customLogger := logtest.NewRecorder()

	tests := []struct {
		name            string
		options         []LoggingOption
		expectedLogs    int
		testCustomLog   bool
		useCustomLogger bool
	}{
		{
			name: "With all options enabled",
			options: []LoggingOption{
				WithLoggingCallStart(true),
				WithLoggingCallHeaders(map[string]string{"custom-header": "custom_header_field"}),
				WithLoggingAddCallInfoToLogger(true),
				WithLoggingSlowCallThreshold(50 * time.Millisecond),
			},
			expectedLogs: 2,
		},
		{
			name: "With excluded methods",
			options: []LoggingOption{
				WithLoggingCallStart(true),
				WithLoggingExcludedMethods("/grpc.testing.TestService/UnaryCall"),
			},
			expectedLogs: 0, // Should be excluded
		},
		{
			name: "With custom logger provider",
			options: []LoggingOption{
				WithLoggingCallStart(true),
				WithLoggingCustomLoggerProvider(func(ctx context.Context, info *grpc.UnaryServerInfo) log.FieldLogger {
					return customLogger
				}),
			},
			expectedLogs:    2,
			testCustomLog:   true,
			useCustomLogger: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.Reset()
			customLogger.Reset()

			_, client, closeSvc, err := startTestService(
				[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
					RequestIDServerUnaryInterceptor(),
					LoggingServerUnaryInterceptor(logger, tt.options...),
				)},
				[]grpc.DialOption{grpc.WithUserAgent(headerUserAgent)})
			require.NoError(t, err)
			defer func() { require.NoError(t, closeSvc()) }()

			md := metadata.Pairs(
				headerRequestIDKey, headerRequestID,
				"custom-header", headerCustomValue,
			)
			reqCtx := metadata.NewOutgoingContext(context.Background(), md)

			// Add trace ID to context
			reqCtx = NewContextWithTraceID(reqCtx, headerTraceID)

			_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
			require.NoError(t, err)

			if tt.testCustomLog && tt.useCustomLogger {
				require.Equal(t, tt.expectedLogs, len(customLogger.Entries()))
				if len(customLogger.Entries()) > 0 {
					// Verify custom logger was used
					logEntry := customLogger.Entries()[0]
					require.Contains(t, logEntry.Text, "gRPC call started")
				}
			} else {
				require.Equal(t, tt.expectedLogs, len(logger.Entries()))
				if len(logger.Entries()) > 0 {
					logEntry := logger.Entries()[0]
					if tt.name == "With all options enabled" {
						require.Contains(t, logEntry.Text, "gRPC call started")
						// Check custom headers
						requireLogFieldString(t, logEntry, "custom_header_field", headerCustomValue)
					}
				}
			}
		})
	}
}

func TestLoggingServerUnaryInterceptor_ExcludedMethods(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	tests := []struct {
		name         string
		method       string
		statusCode   codes.Code
		expectedLogs int
	}{
		{
			name:         "Excluded method with success",
			method:       "/grpc.testing.TestService/UnaryCall",
			statusCode:   codes.OK,
			expectedLogs: 0,
		},
		{
			name:         "Excluded method with error",
			method:       "/grpc.testing.TestService/UnaryCall",
			statusCode:   codes.Internal,
			expectedLogs: 1, // Should log errors even for excluded methods
		},
		{
			name:         "Non-excluded method",
			method:       "/grpc.testing.TestService/UnaryCall",
			statusCode:   codes.OK,
			expectedLogs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger.Reset()

			// Use different exclusion based on test case
			excludedMethod := "/grpc.testing.TestService/UnaryCall"
			if tt.name == "Non-excluded method" {
				excludedMethod = "/grpc.testing.TestService/DifferentCall"
			}

			svc, client, closeSvc, err := startTestService(
				[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
					RequestIDServerUnaryInterceptor(),
					LoggingServerUnaryInterceptor(logger,
						WithLoggingExcludedMethods(excludedMethod),
					),
				)},
				nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, closeSvc()) }()

			if tt.statusCode != codes.OK {
				svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
					return nil, status.Error(tt.statusCode, "test error")
				})
			}

			reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
			_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
			if tt.statusCode != codes.OK {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.expectedLogs, len(logger.Entries()))
		})
	}
}

func TestLoggingServerUnaryInterceptor_SlowRequests(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	svc, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger,
				WithLoggingSlowCallThreshold(10*time.Millisecond),
			),
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	// Set up a slow handler
	svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
		time.Sleep(50 * time.Millisecond) // Simulate slow processing
		return &grpc_testing.SimpleResponse{Payload: &grpc_testing.Payload{Body: []byte("test")}}, nil
	})

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	require.Equal(t, 1, len(logger.Entries()))
	logEntry := logger.Entries()[0]
	require.Contains(t, logEntry.Text, "gRPC call finished")

	// Check for slow request flag
	slowField, found := logEntry.FindField("slow_request")
	require.True(t, found)
	require.True(t, slowField.Int != 0)

	// Check for time_slots field
	_, found = logEntry.FindField("time_slots")
	require.True(t, found)
}

func TestLoggingServerUnaryInterceptor_LoggingParams(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	// Create a custom interceptor that adds fields to logging params
	customInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		lp := GetLoggingParamsFromContext(ctx)
		if lp != nil {
			lp.ExtendFields(
				log.String("custom_field_1", "value1"),
				log.Int("custom_field_2", 42),
			)
		}
		return handler(ctx, req)
	}

	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger),
			customInterceptor,
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	require.Equal(t, 1, len(logger.Entries()))
	logEntry := logger.Entries()[0]
	require.Contains(t, logEntry.Text, "gRPC call finished")

	// Check for custom fields from LoggingParams
	requireLogFieldString(t, logEntry, "custom_field_1", "value1")
	requireLogFieldInt(t, logEntry, "custom_field_2", 42)
}

func TestLoggingServerUnaryInterceptor_RemoteAddressParsing(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger),
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	require.Equal(t, 1, len(logger.Entries()))
	logEntry := logger.Entries()[0]

	// Check that remote address fields are present
	remoteAddrField, found := logEntry.FindField("remote_addr")
	require.True(t, found)
	require.True(t, strings.HasPrefix(string(remoteAddrField.Bytes), "127.0.0.1:"))

	// Check parsed IP and port
	ipField, found := logEntry.FindField("remote_addr_ip")
	require.True(t, found)
	require.Equal(t, "127.0.0.1", string(ipField.Bytes))

	portField, found := logEntry.FindField("remote_addr_port")
	require.True(t, found)
	require.Greater(t, uint16(portField.Int), uint16(0))
}

func TestLoggingServerUnaryInterceptor_RequestHeaders(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger,
				WithLoggingCallHeaders(map[string]string{
					"x-custom-header":  "custom_header_log",
					"x-missing-header": "missing_header_log",
				}),
			),
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	md := metadata.Pairs(
		headerRequestIDKey, headerRequestID,
		"x-custom-header", "custom-value",
	)
	reqCtx := metadata.NewOutgoingContext(context.Background(), md)

	_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	require.Equal(t, 1, len(logger.Entries()))
	logEntry := logger.Entries()[0]

	// Check that custom header is logged
	requireLogFieldString(t, logEntry, "custom_header_log", "custom-value")

	// Check that missing header is not logged (field should not exist)
	_, found := logEntry.FindField("missing_header_log")
	require.False(t, found)
}

func TestLoggingServerUnaryInterceptor_AddRequestInfoToLogger(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	// Create a custom interceptor that uses the logger from context
	var contextLogger log.FieldLogger
	customInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		contextLogger = GetLoggerFromContext(ctx)
		return handler(ctx, req)
	}

	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger, WithLoggingAddCallInfoToLogger(true)),
			customInterceptor,
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
	require.NoError(t, err)

	require.NotNil(t, contextLogger)

	// Test that the context logger has the request info
	// This is harder to test directly, but we can verify that the logger was properly configured
	require.Equal(t, 1, len(logger.Entries()))
	logEntry := logger.Entries()[0]
	requireLogFieldString(t, logEntry, "request_id", headerRequestID)
}

func TestLoggingServerUnaryInterceptor_Errors(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	svc, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger),
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	// Set up an error handler
	testErr := status.Error(codes.Internal, "test internal error")
	svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
		return nil, testErr
	})

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
	require.Error(t, err)

	require.Equal(t, 1, len(logger.Entries()))
	logEntry := logger.Entries()[0]
	require.Contains(t, logEntry.Text, "gRPC call finished")

	// Check error fields
	requireLogFieldString(t, logEntry, "grpc_code", codes.Internal.String())
	requireLogFieldString(t, logEntry, "grpc_error", "rpc error: code = Internal desc = test internal error")
}

func requireLogFieldString(t *testing.T, logEntry logtest.RecordedEntry, key, want string) {
	t.Helper()
	logField, found := logEntry.FindField(key)
	require.True(t, found)
	require.Equal(t, want, string(logField.Bytes))
}

func requireLogFieldInt(t *testing.T, logEntry logtest.RecordedEntry, key string, want int) {
	t.Helper()
	logField, found := logEntry.FindField(key)
	require.True(t, found)
	require.Equal(t, want, int(logField.Int))
}

func getLogFieldAsString(logEntry logtest.RecordedEntry, key string) string {
	logField, found := logEntry.FindField(key)
	if !found {
		return ""
	}
	return string(logField.Bytes)
}

func TestLoggingServerStreamInterceptor(t *testing.T) {
	const headerRequestID = "test-request-id"
	const headerUserAgent = "test-user-agent"

	logger := logtest.NewRecorder()

	svc, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainStreamInterceptor(RequestIDServerStreamInterceptor(), LoggingServerStreamInterceptor(logger))},
		[]grpc.DialOption{grpc.WithUserAgent(headerUserAgent)})
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	requireCommonFields := func(t *testing.T, logEntry logtest.RecordedEntry) {
		requireLogFieldString(t, logEntry, "request_id", headerRequestID)
		require.NotEmpty(t, getLogFieldAsString(logEntry, "int_request_id"))
		requireLogFieldString(t, logEntry, "grpc_service", "grpc.testing.TestService")
		requireLogFieldString(t, logEntry, "grpc_method", "StreamingOutputCall")
		requireLogFieldString(t, logEntry, "grpc_method_type", string(CallMethodTypeStream))
		require.True(t, strings.HasPrefix(getLogFieldAsString(logEntry, "remote_addr"), "127.0.0.1:"))
		requireLogFieldString(t, logEntry, "user_agent", headerUserAgent+" grpc-go/"+grpc.Version)
	}

	permissionDeniedErr := status.Error(codes.PermissionDenied, "Permission denied")

	tests := []struct {
		name                       string
		md                         metadata.MD
		streamingOutputCallHandler func(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error
		wantErr                    error
		wantCode                   codes.Code
	}{
		{
			name:     "log gRPC stream call is started and finished",
			md:       metadata.Pairs(headerRequestIDKey, headerRequestID),
			wantCode: codes.OK,
		},
		{
			name: "log gRPC stream call is started and finished with error",
			md:   metadata.Pairs(headerRequestIDKey, headerRequestID),
			streamingOutputCallHandler: func(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error {
				return permissionDeniedErr
			},
			wantErr:  permissionDeniedErr,
			wantCode: codes.PermissionDenied,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.streamingOutputCallHandler != nil {
				svc.SwitchStreamingOutputCallHandler(tt.streamingOutputCallHandler)
			}

			reqCtx := metadata.NewOutgoingContext(context.Background(), tt.md)
			stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
			require.NoError(t, streamErr)

			// Receive one message to trigger the interceptor
			_, recvErr := stream.Recv()
			if tt.wantErr != nil {
				require.ErrorIs(t, recvErr, tt.wantErr)
			} else {
				require.NoError(t, recvErr)
			}

			require.Equal(t, 1, len(logger.Entries()))

			callFinishedLogEntry := logger.Entries()[0]
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

func TestLoggingServerStreamInterceptorWithOptions(t *testing.T) {
	const headerRequestID = "test-request-id"
	const headerUserAgent = "test-user-agent"

	logger := logtest.NewRecorder()

	// Test with request start enabled
	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainStreamInterceptor(
			RequestIDServerStreamInterceptor(),
			LoggingServerStreamInterceptor(logger, WithLoggingCallStart(true)),
		)},
		[]grpc.DialOption{grpc.WithUserAgent(headerUserAgent)})
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
	require.NoError(t, streamErr)

	// Receive one message to trigger the interceptor
	_, recvErr := stream.Recv()
	require.NoError(t, recvErr)

	// Should have 2 entries: started and finished
	require.Equal(t, 2, len(logger.Entries()))
	require.Contains(t, logger.Entries()[0].Text, "gRPC call started")
	require.Contains(t, logger.Entries()[1].Text, "gRPC call finished")
}

func TestLoggingServerStreamInterceptor_ExcludedMethods(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainStreamInterceptor(
			RequestIDServerStreamInterceptor(),
			LoggingServerStreamInterceptor(logger,
				WithLoggingExcludedMethods("/grpc.testing.TestService/StreamingOutputCall"),
			),
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
	require.NoError(t, streamErr)

	// Receive one message to trigger the interceptor
	_, recvErr := stream.Recv()
	require.NoError(t, recvErr)

	// Should have no log entries for excluded method
	require.Equal(t, 0, len(logger.Entries()))
}

func TestLoggingServerStreamInterceptor_SlowRequests(t *testing.T) {
	const headerRequestID = "test-request-id"

	logger := logtest.NewRecorder()

	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainStreamInterceptor(
			RequestIDServerStreamInterceptor(),
			LoggingServerStreamInterceptor(logger,
				WithLoggingSlowCallThreshold(1*time.Nanosecond), // Very low threshold to trigger slow request
			),
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
	require.NoError(t, streamErr)

	// Receive one message to trigger the interceptor
	_, recvErr := stream.Recv()
	require.NoError(t, recvErr)

	require.Equal(t, 1, len(logger.Entries()))
	logEntry := logger.Entries()[0]

	// Should have slow_request field set to true
	field, found := logEntry.FindField("slow_request")
	require.True(t, found)
	require.True(t, field.Int != 0)
}

func TestLoggingServerStreamInterceptor_CustomStreamLoggerProvider(t *testing.T) {
	const headerRequestID = "test-request-id"

	baseLogger := logtest.NewRecorder()
	customLogger := logtest.NewRecorder()

	customProvider := func(ctx context.Context, info *grpc.StreamServerInfo) log.FieldLogger {
		if info.FullMethod == "/grpc.testing.TestService/StreamingOutputCall" {
			return customLogger
		}
		return nil
	}

	_, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainStreamInterceptor(
			RequestIDServerStreamInterceptor(),
			LoggingServerStreamInterceptor(baseLogger, WithLoggingCustomStreamLoggerProvider(customProvider)),
		)},
		nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	reqCtx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(headerRequestIDKey, headerRequestID))
	stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
	require.NoError(t, streamErr)

	// Receive one message to trigger the interceptor
	_, recvErr := stream.Recv()
	require.NoError(t, recvErr)

	// Base logger should have no entries, custom logger should have 1
	require.Equal(t, 0, len(baseLogger.Entries()))
	require.Equal(t, 1, len(customLogger.Entries()))
	require.Contains(t, customLogger.Entries()[0].Text, "gRPC call finished")
}
