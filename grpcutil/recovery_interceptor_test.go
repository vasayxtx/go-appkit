package grpcutil

import (
	"context"
	"testing"

	"github.com/acronis/go-appkit/log/logtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
)

func TestRecoveryServerUnaryInterceptor(t *testing.T) {
	logger := logtest.NewRecorder()

	svc, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
			RequestIDServerUnaryInterceptor(),
			LoggingServerUnaryInterceptor(logger),
			RecoveryServerUnaryInterceptor(),
		)}, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
		panic("test")
	})

	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	require.ErrorIs(t, err, InternalError)

	require.Equal(t, 3, len(logger.Entries()))
	require.Contains(t, logger.Entries()[0].Text, "gRPC call started")
	require.Contains(t, logger.Entries()[2].Text, "gRPC call finished")
	require.Contains(t, logger.Entries()[1].Text, "Panic: test")
	require.NotEmpty(t, getLogFieldAsString(logger.Entries()[1], "stack"))
}
