/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
)

func TestRequestIDServerUnaryInterceptor(t *testing.T) {
	getStringFromMd := func(t *testing.T, md metadata.MD, key string) string {
		vals := md.Get(key)
		require.Len(t, vals, 1)
		return vals[0]
	}

	tests := []struct {
		name        string
		options     []RequestIDOption
		md          metadata.MD
		checkFn     func(t *testing.T, svc *testService, header metadata.MD)
	}{
		{
			name:    "Default generators - Request ID not present in request header metadata",
			options: nil,
			md:      metadata.Pairs(),
			checkFn: func(t *testing.T, svc *testService, respHeader metadata.MD) {
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
			name:    "Default generators - Request ID present in metadata",
			options: nil,
			md:      metadata.Pairs(headerRequestIDKey, "existing-request-id"),
			checkFn: func(t *testing.T, svc *testService, respHeader metadata.MD) {
				reqID := getStringFromMd(t, respHeader, headerRequestIDKey)
				require.Equal(t, "existing-request-id", reqID)
				require.Equal(t, reqID, GetRequestIDFromContext(svc.lastCtx))

				intReqID := getStringFromMd(t, respHeader, headerRequestInternalIDKey)
				require.NotEmpty(t, intReqID)
				require.Equal(t, intReqID, GetInternalRequestIDFromContext(svc.lastCtx))

				require.NotEqual(t, reqID, intReqID)
			},
		},
		{
			name: "Custom generators - Request ID not present",
			options: []RequestIDOption{
				WithRequestIDGenerator(func() string { return "custom-request-id" }),
				WithInternalRequestIDGenerator(func() string { return "custom-internal-id" }),
			},
			md: metadata.Pairs(),
			checkFn: func(t *testing.T, svc *testService, respHeader metadata.MD) {
				reqID := getStringFromMd(t, respHeader, headerRequestIDKey)
				require.Equal(t, "custom-request-id", reqID)
				require.Equal(t, reqID, GetRequestIDFromContext(svc.lastCtx))

				intReqID := getStringFromMd(t, respHeader, headerRequestInternalIDKey)
				require.Equal(t, "custom-internal-id", intReqID)
				require.Equal(t, intReqID, GetInternalRequestIDFromContext(svc.lastCtx))
			},
		},
		{
			name: "Custom generators - Existing request ID preserved, custom internal ID generator used",
			options: []RequestIDOption{
				WithRequestIDGenerator(func() string { return "custom-request-id" }),
				WithInternalRequestIDGenerator(func() string { return "custom-internal-id" }),
			},
			md: metadata.Pairs(headerRequestIDKey, "existing-request-id"),
			checkFn: func(t *testing.T, svc *testService, respHeader metadata.MD) {
				reqID := getStringFromMd(t, respHeader, headerRequestIDKey)
				require.Equal(t, "existing-request-id", reqID)
				require.Equal(t, reqID, GetRequestIDFromContext(svc.lastCtx))

				intReqID := getStringFromMd(t, respHeader, headerRequestInternalIDKey)
				require.Equal(t, "custom-internal-id", intReqID)
				require.Equal(t, intReqID, GetInternalRequestIDFromContext(svc.lastCtx))
			},
		},
		{
			name: "Custom request ID generator only",
			options: []RequestIDOption{
				WithRequestIDGenerator(func() string { return "custom-request-id" }),
			},
			md: metadata.Pairs(),
			checkFn: func(t *testing.T, svc *testService, respHeader metadata.MD) {
				reqIDVals := respHeader.Get(headerRequestIDKey)
				require.Len(t, reqIDVals, 1)
				require.Equal(t, "custom-request-id", reqIDVals[0])

				intReqIDVals := respHeader.Get(headerRequestInternalIDKey)
				require.Len(t, intReqIDVals, 1)
				require.NotEmpty(t, intReqIDVals[0])
				require.NotEqual(t, "custom-request-id", intReqIDVals[0]) // Should use default generator
			},
		},
		{
			name: "Custom internal request ID generator only",
			options: []RequestIDOption{
				WithInternalRequestIDGenerator(func() string { return "custom-internal-id" }),
			},
			md: metadata.Pairs(),
			checkFn: func(t *testing.T, svc *testService, respHeader metadata.MD) {
				reqIDVals := respHeader.Get(headerRequestIDKey)
				require.Len(t, reqIDVals, 1)
				require.NotEmpty(t, reqIDVals[0])
				require.NotEqual(t, "custom-internal-id", reqIDVals[0]) // Should use default generator

				intReqIDVals := respHeader.Get(headerRequestInternalIDKey)
				require.Len(t, intReqIDVals, 1)
				require.Equal(t, "custom-internal-id", intReqIDVals[0])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, client, closeSvc, err := startTestService([]grpc.ServerOption{
				grpc.UnaryInterceptor(RequestIDServerUnaryInterceptor(tt.options...)),
			}, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, closeSvc()) }()

			var header metadata.MD
			reqCtx := metadata.NewOutgoingContext(context.Background(), tt.md)
			resp, respErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{}, grpc.Header(&header))
			require.NoError(t, respErr)
			require.Equal(t, "test", string(resp.Payload.GetBody()))
			tt.checkFn(t, svc, header)
		})
	}
}
