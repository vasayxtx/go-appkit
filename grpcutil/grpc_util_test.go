package grpcutil

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/interop/grpc_testing"

	"github.com/acronis/go-appkit/grpcutil/grpctest"
)

type testService struct {
	grpc_testing.UnimplementedTestServiceServer
	lastCtx          context.Context
	unaryCallHandler func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error)
}

func (s *testService) UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	s.lastCtx = ctx
	if s.unaryCallHandler != nil {
		return s.unaryCallHandler(ctx, req)
	}
	return &grpc_testing.SimpleResponse{Payload: &grpc_testing.Payload{Body: []byte("test")}}, nil
}

func (s *testService) SwitchUnaryCallHandler(
	handler func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error),
) {
	s.unaryCallHandler = handler
}

func (s *testService) Reset() {
	s.lastCtx = nil
	s.unaryCallHandler = nil
}

func startTestService(
	serverOpts []grpc.ServerOption,
	dialOpts []grpc.DialOption,
) (svc *testService, client grpc_testing.TestServiceClient, closeFn func() error, err error) {
	svc = &testService{}
	var clientConn *grpc.ClientConn
	if _, clientConn, closeFn, err = grpctest.NewServerAndClient(serverOpts, dialOpts, func(s *grpc.Server) {
		grpc_testing.RegisterTestServiceServer(s, svc)
	}); err != nil {
		return nil, nil, nil, err
	}
	return svc, grpc_testing.NewTestServiceClient(clientConn), closeFn, nil
}
