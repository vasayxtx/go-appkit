package grpcutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

func TestRequestIDServerUnaryInterceptor(t *testing.T) {
	svc, client, closeSvc, err := startTestService([]grpc.ServerOption{grpc.UnaryInterceptor(RequestIDServerUnaryInterceptor())}, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, closeSvc()) }()

	getStringFromMd := func(t *testing.T, md metadata.MD, key string) string {
		vals := md.Get(key)
		require.Len(t, vals, 1)
		return vals[0]
	}

	tests := []struct {
		name    string
		md      metadata.MD
		checkFn func(t *testing.T, header metadata.MD)
	}{
		{
			name: "Request ID not present in request header metadata",
			md:   metadata.Pairs(),
			checkFn: func(t *testing.T, respHeader metadata.MD) {
				reqID := getStringFromMd(t, respHeader, headerRequestIDKey)
				require.NotEmpty(t, reqID)
				require.Equal(t, reqID, GetRequestIDFromContext(svc.lastCtx))

				intReqID := getStringFromMd(t, respHeader, headerRequestInternalIDKey)
				require.NotEmpty(t, intReqID)
				require.Equal(t, intReqID, GetInternalRequestIDFromContext(svc.lastCtx))

				require.NotEqual(t, reqID, intReqID)
			},
		},
		{
			name: "Request ID present in metadata",
			md:   metadata.Pairs(headerRequestIDKey, "existing-request-id"),
			checkFn: func(t *testing.T, respHeader metadata.MD) {
				reqID := getStringFromMd(t, respHeader, headerRequestIDKey)
				require.Equal(t, "existing-request-id", reqID)
				require.Equal(t, reqID, GetRequestIDFromContext(svc.lastCtx))

				intReqID := getStringFromMd(t, respHeader, headerRequestInternalIDKey)
				require.NotEmpty(t, intReqID)
				require.Equal(t, intReqID, GetInternalRequestIDFromContext(svc.lastCtx))

				require.NotEqual(t, reqID, intReqID)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var header metadata.MD
			reqCtx := metadata.NewOutgoingContext(context.Background(), tt.md)
			resp, respErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{}, grpc.Header(&header))
			require.NoError(t, respErr)
			require.Equal(t, "test", string(resp.Payload.GetBody()))
			tt.checkFn(t, header)
			svc.Reset()
		})
	}
}
