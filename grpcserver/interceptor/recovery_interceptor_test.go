/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"testing"

	"github.com/acronis/go-appkit/log/logtest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
)

func TestRecoveryServerUnaryInterceptorWithStackSize(t *testing.T) {
	tests := []struct {
		name      string
		option    RecoveryOption
		wantStack bool
	}{
		{
			name:      "Default stack size",
			option:    nil,
			wantStack: true, // Default stack size is > 0
		},
		{
			name:      "Custom stack size",
			option:    WithRecoveryStackSize(4096),
			wantStack: true,
		},
		{
			name:      "Zero stack size",
			option:    WithRecoveryStackSize(0),
			wantStack: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logtest.NewRecorder()

			var interceptor grpc.UnaryServerInterceptor
			if tt.option != nil {
				interceptor = RecoveryServerUnaryInterceptor(tt.option)
			} else {
				interceptor = RecoveryServerUnaryInterceptor()
			}

			svc, client, closeSvc, err := startTestService(
				[]grpc.ServerOption{grpc.ChainUnaryInterceptor(
					RequestIDServerUnaryInterceptor(),
					LoggingServerUnaryInterceptor(logger),
					interceptor,
				)}, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, closeSvc()) }()

			svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
				panic("test panic")
			})

			_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
			require.ErrorIs(t, err, InternalError)

			// Should have 2 log entries: panic log and request finished log
			require.Equal(t, 2, len(logger.Entries()))
			panicEntry := logger.Entries()[0]
			require.Contains(t, panicEntry.Text, "Panic: test panic")

			stackField := getLogFieldAsString(panicEntry, "stack")
			if tt.wantStack {
				require.NotEmpty(t, stackField)
				require.Contains(t, stackField, "panic") // Stack trace should contain panic info
			} else {
				// When stack size is 0, stack field should be empty
				require.Empty(t, stackField)
			}
		})
	}
}

func TestRecoveryServerUnaryInterceptorNoLogger(t *testing.T) {
	// Test behavior when no logger is available in context
	svc, client, closeSvc, err := startTestService(
		[]grpc.ServerOption{grpc.UnaryInterceptor(RecoveryServerUnaryInterceptor())}, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
		panic("test panic")
	})

	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	require.ErrorIs(t, err, InternalError)
	// Should not panic or cause issues when no logger is present
}
