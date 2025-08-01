/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package throttle

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/acronis/go-appkit/grpcserver/interceptor"
	"github.com/acronis/go-appkit/log/logtest"
)

// grpcTestService represents the gRPC test service interface
type grpcTestService interface {
	UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error)
	StreamingOutputCall(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error
	EmptyCall(ctx context.Context, req *grpc_testing.Empty) (*grpc_testing.Empty, error)
}

// testService implements grpcTestService
type testService struct {
	grpc_testing.UnimplementedTestServiceServer
	unaryCallHandler           func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error)
	streamingOutputCallHandler func(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error
}

func (s *testService) UnaryCall(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	if s.unaryCallHandler != nil {
		return s.unaryCallHandler(ctx, req)
	}
	return &grpc_testing.SimpleResponse{Payload: &grpc_testing.Payload{Body: []byte("test")}}, nil
}

func (s *testService) StreamingOutputCall(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error {
	if s.streamingOutputCallHandler != nil {
		return s.streamingOutputCallHandler(req, stream)
	}
	return stream.Send(&grpc_testing.StreamingOutputCallResponse{
		Payload: &grpc_testing.Payload{Body: []byte("test-stream")},
	})
}

func (s *testService) EmptyCall(ctx context.Context, req *grpc_testing.Empty) (*grpc_testing.Empty, error) {
	return &grpc_testing.Empty{}, nil
}

func startTestService(
	serverOpts []grpc.ServerOption,
	dialOpts []grpc.DialOption,
) (svc grpcTestService, client grpc_testing.TestServiceClient, closeFn func() error, err error) {
	testSvc := &testService{}
	var clientConn *grpc.ClientConn
	if _, clientConn, closeFn, err = newTestServerAndClient(serverOpts, dialOpts, func(s *grpc.Server) {
		grpc_testing.RegisterTestServiceServer(s, testSvc)
	}); err != nil {
		return nil, nil, nil, err
	}
	return testSvc, grpc_testing.NewTestServiceClient(clientConn), closeFn, nil
}

func newTestServerAndClient(
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
			if srvErr := <-serveResult; srvErr != nil && !isConnClosedError(srvErr) {
				err = fmt.Errorf("serve: %w; %w", srvErr, err)
			}
		}
	}()

	// Create client connection with insecure credentials
	clientConn, dialErr := grpc.NewClient(ln.Addr().String(),
		append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))...,
	)
	if dialErr != nil {
		return nil, nil, nil, fmt.Errorf("dial: %w", dialErr)
	}
	return srv, clientConn, func() error {
		mErr := clientConn.Close()
		srv.GracefulStop()
		return mErr
	}, nil
}

func isConnClosedError(err error) bool {
	return err != nil && err.Error() == "use of closed network connection"
}

// ThrottleInterceptorTestSuite is a test suite for Throttle interceptors
type ThrottleInterceptorTestSuite struct {
	suite.Suite
	IsUnary bool
}

func TestThrottleUnaryInterceptor(t *testing.T) {
	suite.Run(t, &ThrottleInterceptorTestSuite{IsUnary: true})
}

func TestThrottleStreamInterceptor(t *testing.T) {
	suite.Run(t, &ThrottleInterceptorTestSuite{IsUnary: false})
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_RateLimitBasicFunctionality() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				RateLimit: RateLimitValue{Count: 1, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/*"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	if s.IsUnary {
		_, unaryErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
		// Second request should be rejected
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
		// Second request should be rejected
		stream2, streamErr2 := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().Error(recvErr2)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr2))
	}
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_InFlightLimitBasicFunctionality() {
	cfg := &Config{
		InFlightLimitZones: map[string]InFlightLimitZoneConfig{
			"test_zone": {
				InFlightLimit: 1,
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods:  []string{"/grpc.testing.TestService/*"},
				InFlightLimits: []RuleInFlightLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	svc, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	// Add delay to the service handler to ensure requests are actually in-flight
	var requestStarted, requestCanFinish sync.WaitGroup
	requestStarted.Add(1)
	requestCanFinish.Add(1)

	if s.IsUnary {
		svc.(*testService).unaryCallHandler = func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
			requestStarted.Done()
			requestCanFinish.Wait()
			return &grpc_testing.SimpleResponse{Payload: &grpc_testing.Payload{Body: []byte("test")}}, nil
		}
	} else {
		svc.(*testService).streamingOutputCallHandler = func(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error {
			requestStarted.Done()
			requestCanFinish.Wait()
			return stream.Send(&grpc_testing.StreamingOutputCallResponse{
				Payload: &grpc_testing.Payload{Body: []byte("test-stream")},
			})
		}
	}

	var wg sync.WaitGroup
	var okCount, rejectedCount atomic.Int32

	concurrentReqs := 5
	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.IsUnary {
				_, unaryErr := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
				if unaryErr != nil {
					if status.Code(unaryErr) == codes.ResourceExhausted {
						rejectedCount.Inc()
					}
				} else {
					okCount.Inc()
				}
			} else {
				stream, streamErr := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
				if streamErr != nil {
					rejectedCount.Inc()
					return
				}
				_, recvErr := stream.Recv()
				if recvErr != nil {
					if status.Code(recvErr) == codes.ResourceExhausted {
						rejectedCount.Inc()
					}
				} else {
					okCount.Inc()
				}
			}
		}()
	}

	// Wait for the first request to start
	requestStarted.Wait()
	
	// Give a small delay to ensure other requests are queued
	time.Sleep(10 * time.Millisecond)
	
	// Allow the first request to complete
	requestCanFinish.Done()
	
	wg.Wait()

	s.Require().Equal(1, int(okCount.Load()))
	s.Require().Equal(concurrentReqs-1, int(rejectedCount.Load()))
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_RateLimitLeakyBucket() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				Alg:        RateLimitAlgLeakyBucket,
				RateLimit:  RateLimitValue{Count: 5, Duration: time.Second},
				BurstLimit: 5,
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/*"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	concurrentReqs := 10
	var okCount, rejectedCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.IsUnary {
				_, unaryErr := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
				if unaryErr != nil {
					if status.Code(unaryErr) == codes.ResourceExhausted {
						rejectedCount.Inc()
					}
				} else {
					okCount.Inc()
				}
			} else {
				stream, streamErr := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
				if streamErr != nil {
					rejectedCount.Inc()
					return
				}
				_, recvErr := stream.Recv()
				if recvErr != nil {
					if status.Code(recvErr) == codes.ResourceExhausted {
						rejectedCount.Inc()
					}
				} else {
					okCount.Inc()
				}
			}
		}()
	}
	wg.Wait()

	// Should allow burst + 1 request (initial bucket capacity + 1 emission)
	s.Require().Equal(6, int(okCount.Load()))
	s.Require().Equal(concurrentReqs-6, int(rejectedCount.Load()))
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_RateLimitSlidingWindow() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				Alg:       RateLimitAlgSlidingWindow,
				RateLimit: RateLimitValue{Count: 2, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/*"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	concurrentReqs := 5
	var okCount, rejectedCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.IsUnary {
				_, unaryErr := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
				if unaryErr != nil {
					if status.Code(unaryErr) == codes.ResourceExhausted {
						rejectedCount.Inc()
					}
				} else {
					okCount.Inc()
				}
			} else {
				stream, streamErr := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
				if streamErr != nil {
					rejectedCount.Inc()
					return
				}
				_, recvErr := stream.Recv()
				if recvErr != nil {
					if status.Code(recvErr) == codes.ResourceExhausted {
						rejectedCount.Inc()
					}
				} else {
					okCount.Inc()
				}
			}
		}()
	}
	wg.Wait()

	// Should allow exactly 'rate.Count' requests in the sliding window
	s.Require().Equal(2, int(okCount.Load()))
	s.Require().Equal(concurrentReqs-2, int(rejectedCount.Load()))
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_WithIdentityKey() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				RateLimit: RateLimitValue{Count: 1, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeIdentity},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/*"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	getKeyIdentity := func(ctx context.Context, fullMethod string) (string, bool, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if clientIDs := md.Get("client-id"); len(clientIDs) > 0 {
				return clientIDs[0], false, nil
			}
		}
		return "", true, nil // bypass if no client-id
	}

	var opts interface{}
	if s.IsUnary {
		opts = Opts{GetKeyIdentity: getKeyIdentity}
	} else {
		opts = StreamOpts{GetKeyIdentity: getKeyIdentity}
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestServiceWithOpts(logger, cfg, opts)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	// Client 1 requests
	client1Ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("client-id", "client-1"))
	if s.IsUnary {
		_, unaryErr := client.UnaryCall(client1Ctx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
		_, unaryErr2 := client.UnaryCall(client1Ctx, &grpc_testing.SimpleRequest{})
		s.Require().Error(unaryErr2)
		s.Require().Equal(codes.ResourceExhausted, status.Code(unaryErr2))
	} else {
		stream, streamErr := client.StreamingOutputCall(client1Ctx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)

		stream2, streamErr2 := client.StreamingOutputCall(client1Ctx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().Error(recvErr2)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr2))
	}

	// Client 2 should have its own rate limit
	client2Ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("client-id", "client-2"))
	if s.IsUnary {
		_, err = client.UnaryCall(client2Ctx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(client2Ctx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}

	// Request without client-id should bypass rate limiting
	if s.IsUnary {
		_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
		_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)

		stream2, streamErr2 := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().NoError(recvErr2)
	}
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_DryRun() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				RateLimit: RateLimitValue{Count: 1, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key:    ServiceKeyConfig{Type: ServiceKeyTypeMethod},
					DryRun: true,
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/*"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// All requests should succeed in dry run mode
	for i := 0; i < 5; i++ {
		if s.IsUnary {
			_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
			s.Require().NoError(err)
		} else {
			stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
			s.Require().NoError(streamErr)
			_, recvErr := stream.Recv()
			s.Require().NoError(recvErr)
		}
	}
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_ServiceMethodMatching() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				RateLimit: RateLimitValue{Count: 1, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/UnaryCall"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	if s.IsUnary {
		// First UnaryCall should succeed
		_, unaryErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
		// Second UnaryCall should be rejected
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))

		// EmptyCall should not be throttled (not in ServiceMethods)
		_, err = client.EmptyCall(reqCtx, &grpc_testing.Empty{})
		s.Require().NoError(err)
		_, err = client.EmptyCall(reqCtx, &grpc_testing.Empty{})
		s.Require().NoError(err)
	} else {
		// StreamingOutputCall should not be throttled (not in ServiceMethods)
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)

		stream2, streamErr2 := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().NoError(recvErr2)
	}
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_ExcludedServiceMethods() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				RateLimit: RateLimitValue{Count: 1, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods:         []string{"/grpc.testing.TestService/*"},
				ExcludedServiceMethods: []string{"/grpc.testing.TestService/EmptyCall"},
				RateLimits:             []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// EmptyCall should not be throttled (excluded)
	_, err = client.EmptyCall(reqCtx, &grpc_testing.Empty{})
	s.Require().NoError(err)
	_, err = client.EmptyCall(reqCtx, &grpc_testing.Empty{})
	s.Require().NoError(err)

	if s.IsUnary {
		// UnaryCall should be throttled
		_, unaryErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	} else {
		// StreamingOutputCall should be throttled
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)

		stream2, streamErr2 := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().Error(recvErr2)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr2))
	}
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_MultipleZones() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"zone1": {
				RateLimit: RateLimitValue{Count: 2, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
			"zone2": {
				RateLimit: RateLimitValue{Count: 1, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		InFlightLimitZones: map[string]InFlightLimitZoneConfig{
			"inflight_zone": {
				InFlightLimit: 3,
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods:  []string{"/grpc.testing.TestService/*"},
				RateLimits:      []RuleRateLimit{{Zone: "zone1"}, {Zone: "zone2"}},
				InFlightLimits: []RuleInFlightLimit{{Zone: "inflight_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// First request should succeed (allowed by both zones)
	if s.IsUnary {
		_, unaryErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
		// Second request should be rejected by zone2 (limit: 1)
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)

		stream2, streamErr2 := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().Error(recvErr2)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr2))
	}
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_Tags() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				RateLimit: RateLimitValue{Count: 1, Duration: time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/*"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
				Tags:           TagsList{"tag1", "tag2"},
			},
		},
	}

	var opts interface{}
	// Use tag filtering - should not apply any rules
	if s.IsUnary {
		opts = Opts{Tags: []string{"tag3"}}
	} else {
		opts = StreamOpts{Tags: []string{"tag3"}}
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestServiceWithOpts(logger, cfg, opts)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// Both requests should succeed (no matching tags)
	if s.IsUnary {
		_, unaryErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)

		stream2, streamErr2 := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().NoError(recvErr2)
	}

	// Now test with matching tag
	if s.IsUnary {
		opts = Opts{Tags: []string{"tag1"}}
	} else {
		opts = StreamOpts{Tags: []string{"tag1"}}
	}

	_, client2, closeSvc2, err := s.setupTestServiceWithOpts(logger, cfg, opts)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc2()) }()

	// First request should succeed, second should be rejected
	if s.IsUnary {
		_, unaryErr := client2.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
		_, err = client2.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	} else {
		stream, streamErr := client2.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)

		stream2, streamErr2 := client2.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().Error(recvErr2)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr2))
	}
}

func (s *ThrottleInterceptorTestSuite) TestThrottleInterceptor_RetryAfterHeader() {
	cfg := &Config{
		RateLimitZones: map[string]RateLimitZoneConfig{
			"test_zone": {
				RateLimit:          RateLimitValue{Count: 1, Duration: time.Second},
				ResponseRetryAfter: RateLimitRetryAfterValue{Duration: 5 * time.Second},
				ServiceConfig: ServiceConfig{
					Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []RuleConfig{
			{
				ServiceMethods: []string{"/grpc.testing.TestService/*"},
				RateLimits:     []RuleRateLimit{{Zone: "test_zone"}},
			},
		},
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, cfg)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// First request should succeed
	if s.IsUnary {
		_, unaryErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(unaryErr)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}

	// Second request should be rejected with retry-after header
	var headers metadata.MD
	if s.IsUnary {
		_, unaryErr := client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{}, grpc.Header(&headers))
		s.Require().Error(unaryErr)
		s.Require().Equal(codes.ResourceExhausted, status.Code(unaryErr))
	} else {
		stream2, streamErr2 := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{}, grpc.Header(&headers))
		s.Require().NoError(streamErr2)
		_, recvErr2 := stream2.Recv()
		s.Require().Error(recvErr2)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr2))
	}

	// Check retry-after header
	retryAfterVals := headers.Get("retry-after")
	s.Require().Len(retryAfterVals, 1)
	s.Require().Equal("5", retryAfterVals[0])
}

func (s *ThrottleInterceptorTestSuite) setupTestService(
	logger *logtest.Recorder, cfg *Config,
) (grpcTestService, grpc_testing.TestServiceClient, func() error, error) {
	return s.setupTestServiceWithOpts(logger, cfg, nil)
}

func (s *ThrottleInterceptorTestSuite) setupTestServiceWithOpts(
	logger *logtest.Recorder, cfg *Config, opts interface{},
) (grpcTestService, grpc_testing.TestServiceClient, func() error, error) {
	var serverOptions []grpc.ServerOption
	if s.IsUnary {
		var unaryInterceptor grpc.UnaryServerInterceptor
		var err error
		if opts != nil {
			if unaryOpts, ok := opts.(Opts); ok {
				unaryInterceptor, err = UnaryInterceptorWithOpts(cfg, unaryOpts)
			} else {
				return nil, nil, nil, err
			}
		} else {
			unaryInterceptor, err = UnaryInterceptor(cfg)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		loggingInterceptor := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
			return handler(interceptor.NewContextWithLogger(ctx, logger), req)
		}
		serverOptions = append(serverOptions, grpc.ChainUnaryInterceptor(loggingInterceptor, unaryInterceptor))
	} else {
		var streamInterceptor grpc.StreamServerInterceptor
		var err error
		if opts != nil {
			if streamOpts, ok := opts.(StreamOpts); ok {
				streamInterceptor, err = StreamInterceptorWithOpts(cfg, streamOpts)
			} else {
				return nil, nil, nil, err
			}
		} else {
			streamInterceptor, err = StreamInterceptor(cfg)
		}
		if err != nil {
			return nil, nil, nil, err
		}
		loggingInterceptor := func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			wrappedStream := &interceptor.WrappedServerStream{ServerStream: ss, Ctx: interceptor.NewContextWithLogger(ss.Context(), logger)}
			return handler(srv, wrappedStream)
		}
		serverOptions = append(serverOptions, grpc.ChainStreamInterceptor(loggingInterceptor, streamInterceptor))
	}
	return startTestService(serverOptions, nil)
}