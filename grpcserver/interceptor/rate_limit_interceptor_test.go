/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/acronis/go-appkit/log"
	"github.com/acronis/go-appkit/log/logtest"
)

// RateLimitInterceptorTestSuite is a test suite for RateLimit interceptors
type RateLimitInterceptorTestSuite struct {
	suite.Suite
	IsUnary bool
}

func TestRateLimitUnaryInterceptor(t *testing.T) {
	suite.Run(t, &RateLimitInterceptorTestSuite{IsUnary: true})
}

func TestRateLimitStreamInterceptor(t *testing.T) {
	suite.Run(t, &RateLimitInterceptorTestSuite{IsUnary: false})
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_BasicFunctionality() {
	tests := []struct {
		name         string
		rate         Rate
		options      []RateLimitOption
		expectAllow  bool
		expectReject bool
	}{
		{
			name:        "allow first request",
			rate:        Rate{1, time.Second},
			expectAllow: true,
		},
		{
			name:         "reject second request immediately",
			rate:         Rate{1, time.Second},
			expectReject: true,
		},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			logger := logtest.NewRecorder()
			_, client, closeSvc, err := s.setupTestService(logger, tt.rate, tt.options)
			s.Require().NoError(err)
			defer func() { s.Require().NoError(closeSvc()) }()

			reqCtx := context.Background()

			if tt.expectAllow {
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

			if tt.expectReject {
				// Make the first request to consume the rate limit
				if s.IsUnary {
					_, _ = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
					// Second request should be rejected
					_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
					s.Require().Error(err)
					s.Require().Equal(codes.ResourceExhausted, status.Code(err))
				} else {
					stream, _ := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
					_, _ = stream.Recv()
					// Second request should be rejected
					stream2, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
					s.Require().NoError(streamErr)
					_, recvErr := stream2.Recv()
					s.Require().Error(recvErr)
					s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr))
				}
			}
		})
	}
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_LeakyBucket() {
	rate := Rate{5, time.Second} // 5 requests per second
	maxBurst := 5

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitAlg(RateLimitAlgLeakyBucket),
		WithRateLimitMaxBurst(maxBurst),
	})
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	// Send concurrent requests up to burst limit
	concurrentReqs := 10
	var okCount, rejectedCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.IsUnary {
				_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
				if err != nil {
					if status.Code(err) == codes.ResourceExhausted {
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

	// Should allow maxBurst + 1 requests (initial bucket capacity + 1 emission)
	s.Require().Equal(maxBurst+1, int(okCount.Load()))
	s.Require().Equal(concurrentReqs-maxBurst-1, int(rejectedCount.Load()))
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_SlidingWindow() {
	rate := Rate{2, time.Second} // 2 requests per second

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitAlg(RateLimitAlgSlidingWindow),
	})
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	// Send concurrent requests
	concurrentReqs := 5
	var okCount, rejectedCount atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < concurrentReqs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.IsUnary {
				_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
				if err != nil {
					if status.Code(err) == codes.ResourceExhausted {
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
	s.Require().Equal(rate.Count, int(okCount.Load()))
	s.Require().Equal(concurrentReqs-rate.Count, int(rejectedCount.Load()))
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_WithGetKey() {
	rate := Rate{1, time.Second}

	getKeyByClientID := func(ctx context.Context, fullMethod string) (string, bool, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if clientIDs := md.Get("client-id"); len(clientIDs) > 0 {
				return clientIDs[0], false, nil
			}
		}
		return "", true, nil // bypass if no client-id
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitGetKey(getKeyByClientID),
	})
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	// Client 1 requests
	client1Ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("client-id", "client-1"))
	if s.IsUnary {
		_, err = client.UnaryCall(client1Ctx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
		_, err = client.UnaryCall(client1Ctx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
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

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_DryRun() {
	rate := Rate{1, time.Second}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitDryRun(true),
	})
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

	// Should have warning logs about rate limit being exceeded
	s.Require().Greater(len(logger.Entries()), 0)
	foundWarning := false
	for _, entry := range logger.Entries() {
		if entry.Level == log.LevelWarn && entry.Text == "rate limit exceeded, continuing in dry run mode" {
			foundWarning = true
			break
		}
	}
	s.Require().True(foundWarning)
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_Backlog() {
	rate := Rate{1, time.Second}
	backlogLimit := 1

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitBacklogLimit(backlogLimit),
	})
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// First request should succeed immediately
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}

	// Second request should be backlogged and eventually succeed
	startTime := time.Now()
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}
	duration := time.Since(startTime)

	// Should have waited approximately the rate limit duration
	s.Require().GreaterOrEqual(duration, time.Millisecond*800) // Allow some tolerance
	s.Require().LessOrEqual(duration, time.Millisecond*1200)   // Allow some tolerance
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_BacklogTimeout() {
	rate := Rate{1, time.Minute} // Very slow rate
	backlogTimeout := 100 * time.Millisecond

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitBacklogLimit(1),
		WithRateLimitBacklogTimeout(backlogTimeout),
	})
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// First request should succeed
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}

	// Second request should timeout in backlog
	startTime := time.Now()
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().Error(recvErr)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr))
	}
	duration := time.Since(startTime)

	// Should have timed out after the backlog timeout
	s.Require().GreaterOrEqual(duration, backlogTimeout)
	s.Require().LessOrEqual(duration, backlogTimeout+50*time.Millisecond) // Some tolerance
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_CustomCallbacks() {
	rate := Rate{1, time.Second}

	var rejectedCalled bool
	customOnReject := func(ctx context.Context, params RateLimitParams, logger log.FieldLogger) error {
		rejectedCalled = true
		return status.Error(codes.Unavailable, "custom rejection message")
	}
	customOnError := func(ctx context.Context, params RateLimitParams, err error, logger log.FieldLogger) error {
		return status.Error(codes.Aborted, "custom error message")
	}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitOnReject(customOnReject),
		WithRateLimitOnError(customOnError),
	})
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// First request should succeed
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}

	// Second request should be rejected with custom callback
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.Unavailable, status.Code(err))
		s.Require().Contains(err.Error(), "custom rejection message")
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().Error(recvErr)
		s.Require().Equal(codes.Unavailable, status.Code(recvErr))
		s.Require().Contains(recvErr.Error(), "custom rejection message")
	}

	s.Require().True(rejectedCalled)
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_RetryAfterHeader() {
	rate := Rate{1, time.Second}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, nil)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// First request should succeed
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}

	// Second request should be rejected with retry-after header
	var headers metadata.MD
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{}, grpc.Header(&headers))
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{}, grpc.Header(&headers))
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().Error(recvErr)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr))
	}

	// Check retry-after header
	retryAfterHeaders := headers.Get("retry-after")
	s.Require().Len(retryAfterHeaders, 1)
	retryAfterSecs, parseErr := strconv.Atoi(retryAfterHeaders[0])
	s.Require().NoError(parseErr)
	s.Require().Greater(retryAfterSecs, 0)
	s.Require().LessOrEqual(retryAfterSecs, int(math.Ceil(rate.Duration.Seconds())))
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_DefaultGetKeyByIP() {
	rate := Rate{1, time.Second}

	logger := logtest.NewRecorder()
	_, client, closeSvc, err := s.setupTestService(logger, rate, []RateLimitOption{
		WithRateLimitGetKey(DefaultRateLimitGetKeyByIP),
	})
	s.Require().NoError(err)
	defer func() { s.Require().NoError(closeSvc()) }()

	reqCtx := context.Background()

	// First request should succeed
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().NoError(err)
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().NoError(recvErr)
	}

	// Second request from same IP should be rejected
	if s.IsUnary {
		_, err = client.UnaryCall(reqCtx, &grpc_testing.SimpleRequest{})
		s.Require().Error(err)
		s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	} else {
		stream, streamErr := client.StreamingOutputCall(reqCtx, &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(streamErr)
		_, recvErr := stream.Recv()
		s.Require().Error(recvErr)
		s.Require().Equal(codes.ResourceExhausted, status.Code(recvErr))
	}
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_InvalidOptions() {
	rate := Rate{1, time.Second}

	// Test negative backlog limit
	_, err := s.createRateLimitInterceptor(rate, []RateLimitOption{
		WithRateLimitBacklogLimit(-1),
	})
	s.Require().Error(err)
	s.Require().Contains(err.Error(), "backlog limit should not be negative")
}

// Helper methods

func (s *RateLimitInterceptorTestSuite) setupTestService(
	logger *logtest.Recorder, rate Rate, options []RateLimitOption,
) (*testService, grpc_testing.TestServiceClient, func() error, error) {
	interceptor, err := s.createRateLimitInterceptor(rate, options)
	if err != nil {
		return nil, nil, nil, err
	}

	var serverOptions []grpc.ServerOption
	if s.IsUnary {
		serverOptions = []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(
				func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
					return interceptor(NewContextWithLogger(ctx, logger), req, info, handler)
				},
			),
		}
	} else {
		streamInterceptor, streamErr := RateLimitStreamInterceptor(rate, options...)
		if streamErr != nil {
			return nil, nil, nil, streamErr
		}
		serverOptions = []grpc.ServerOption{
			grpc.ChainStreamInterceptor(
				func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					wrappedStream := &WrappedServerStream{
						ServerStream: ss,
						Ctx:          NewContextWithLogger(ss.Context(), logger),
					}
					return streamInterceptor(srv, wrappedStream, info, handler)
				},
			),
		}
	}

	return startTestService(serverOptions, nil)
}

func (s *RateLimitInterceptorTestSuite) createRateLimitInterceptor(
	rate Rate, options []RateLimitOption,
) (func(context.Context, interface{}, *grpc.UnaryServerInfo, grpc.UnaryHandler) (interface{}, error), error) {
	if s.IsUnary {
		return RateLimitUnaryInterceptor(rate, options...)
	}
	// For stream tests, we create a dummy unary interceptor just for error testing
	return RateLimitUnaryInterceptor(rate, options...)
}
