package grpctest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// nolint: staticcheck
func NewServerAndClient(
	serverOpts []grpc.ServerOption, dialOpts []grpc.DialOption, registerFn func(s *grpc.Server),
) (server *grpc.Server, clientConn *grpc.ClientConn, closeFn func() error, err error) {
	srv := grpc.NewServer(serverOpts...)
	registerFn(srv)
	ln, lnErr := net.Listen("tcp", "localhost:0")
	if lnErr != nil {
		return nil, nil, nil, fmt.Errorf("listen: %w", lnErr)
	}
	serveResult := make(chan error)
	go func() {
		serveResult <- srv.Serve(ln)
	}()
	defer func() {
		if err != nil {
			srv.Stop()
			if srvErr := <-serveResult; srvErr != nil {
				err = fmt.Errorf("serve: %w; %w", srvErr, err)
			}
		}
	}()

	dialCtx, dialCtxCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer dialCtxCancel()
	clientConn, dialErr := grpc.DialContext(dialCtx, ln.Addr().String(),
		append(dialOpts, grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))...,
	)
	if dialErr != nil {
		return nil, nil, nil, fmt.Errorf("dial: %w", dialErr)
	}
	return srv, clientConn, func() error {
		mErr := clientConn.Close()
		srv.GracefulStop()
		return errors.Join(mErr, <-serveResult)
	}, nil
}
