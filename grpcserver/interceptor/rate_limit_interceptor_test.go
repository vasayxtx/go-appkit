/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"

	"github.com/acronis/go-appkit/log"
	"github.com/acronis/go-appkit/log/logtest"
)

type RateLimitInterceptorTestSuite struct {
	suite.Suite
}

func TestRateLimitUnaryInterceptor(t *testing.T) {
	suite.Run(t, &RateLimitInterceptorTestSuite{})
}

func TestRateLimitStreamInterceptor(t *testing.T) {
	suite.Run(t, &RateLimitInterceptorTestSuite{})
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitUnaryInterceptor_BasicFunctionality() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 2, Duration: time.Second}
	
	// Setup test service with rate limiting
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, nil)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// First two requests should succeed
	for i := 0; i < 2; i++ {
		_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err, "Request %d should succeed", i+1)
	}

	// Third request should be rate limited
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.ResourceExhausted, status.Code(err))
	s.Require().Contains(err.Error(), "rate limit exceeded")

	// Verify logging
	entries := logger.Entries()
	s.Require().True(len(entries) > 0)
	found := false
	for _, entry := range entries {
		if entry.Level == log.LevelWarn && entry.Text == "gRPC request rate limited" {
			found = true
			break
		}
	}
	s.Require().True(found, "Should log rate limit warning")

	_ = testSvc // Avoid unused variable warning
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitStreamInterceptor_BasicFunctionality() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 2, Duration: time.Second}
	
	// Setup test service with rate limiting for streams
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimitStream(logger, rate, nil)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// First two requests should succeed
	for i := 0; i < 2; i++ {
		stream, err := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
		s.Require().NoError(err, "Request %d should succeed", i+1)
		stream.CloseSend()
	}

	// Third request should be rate limited
	_, err = client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.ResourceExhausted, status.Code(err))

	_ = testSvc // Avoid unused variable warning
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_LeakyBucketAlgorithm() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 5, Duration: time.Second}
	options := []RateLimitOption{
		WithRateLimitAlgorithm(RateLimitAlgLeakyBucket),
		WithRateLimitMaxBurst(3),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// Should allow burst of 3 requests
	for i := 0; i < 3; i++ {
		_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err, "Burst request %d should succeed", i+1)
	}

	// Fourth request should be rate limited
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.ResourceExhausted, status.Code(err))

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_SlidingWindowAlgorithm() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 2, Duration: time.Second}
	options := []RateLimitOption{
		WithRateLimitAlgorithm(RateLimitAlgSlidingWindow),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// First two requests should succeed
	for i := 0; i < 2; i++ {
		_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err, "Request %d should succeed", i+1)
	}

	// Third request should be rate limited
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.ResourceExhausted, status.Code(err))

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_DryRunMode() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 1, Duration: time.Second}
	options := []RateLimitOption{
		WithRateLimitDryRun(true),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// All requests should succeed in dry run mode
	for i := 0; i < 5; i++ {
		_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err, "Request %d should succeed in dry run", i+1)
	}

	// Should log dry run messages
	entries := logger.Entries()
	found := false
	for _, entry := range entries {
		if entry.Level == log.LevelInfo && entry.Text == "gRPC request would be rate limited (dry run)" {
			found = true
			break
		}
	}
	s.Require().True(found, "Should log dry run messages")

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_CustomKeyFunction() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 1, Duration: time.Second}
	
	// Custom key function that always returns the same key
	customKeyFunc := func(ctx context.Context, fullMethod string) (string, bool, error) {
		return "custom-key", false, nil
	}
	
	options := []RateLimitOption{
		WithRateLimitGetKey(customKeyFunc),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// First request should succeed
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().NoError(err)

	// Second request should be rate limited
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.ResourceExhausted, status.Code(err))

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_BypassFunction() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 1, Duration: time.Second}
	
	// Key function that bypasses rate limiting
	bypassKeyFunc := func(ctx context.Context, fullMethod string) (string, bool, error) {
		return "key", true, nil // bypass = true
	}
	
	options := []RateLimitOption{
		WithRateLimitGetKey(bypassKeyFunc),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// All requests should succeed due to bypass
	for i := 0; i < 5; i++ {
		_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err, "Request %d should succeed due to bypass", i+1)
	}

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_KeyFunctionError() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 10, Duration: time.Second}
	
	// Key function that returns an error
	errorKeyFunc := func(ctx context.Context, fullMethod string) (string, bool, error) {
		return "", false, errors.New("key extraction error")
	}
	
	options := []RateLimitOption{
		WithRateLimitGetKey(errorKeyFunc),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// Request should fail due to key extraction error
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.Internal, status.Code(err))

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_CustomOnReject() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 1, Duration: time.Second}
	
	customOnReject := func(ctx context.Context, fullMethod string, params RateLimitParams, logger log.FieldLogger) error {
		return status.Error(codes.Unavailable, "custom rejection message")
	}
	
	options := []RateLimitOption{
		WithRateLimitOnReject(customOnReject),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// First request should succeed
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().NoError(err)

	// Second request should be rejected with custom error
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.Unavailable, status.Code(err))
	s.Require().Contains(err.Error(), "custom rejection message")

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_CustomOnError() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 10, Duration: time.Second}
	
	customOnError := func(ctx context.Context, fullMethod string, params RateLimitParams, err error, logger log.FieldLogger) error {
		return status.Error(codes.FailedPrecondition, "custom error message")
	}
	
	// Key function that returns an error to trigger onError
	errorKeyFunc := func(ctx context.Context, fullMethod string) (string, bool, error) {
		return "", false, errors.New("key error")
	}
	
	options := []RateLimitOption{
		WithRateLimitGetKey(errorKeyFunc),
		WithRateLimitOnError(customOnError),
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// Request should fail with custom error
	_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
	s.Require().Error(err)
	s.Require().Equal(codes.FailedPrecondition, status.Code(err))
	s.Require().Contains(err.Error(), "custom error message")

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_ConcurrentRequests() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 5, Duration: time.Second}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, nil)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	const numGoroutines = 10
	const requestsPerGoroutine = 2
	
	var wg sync.WaitGroup
	var successCount, errorCount int32
	var mu sync.Mutex
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < requestsPerGoroutine; j++ {
				_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
				mu.Lock()
				if err != nil {
					errorCount++
				} else {
					successCount++
				}
				mu.Unlock()
			}
		}()
	}
	
	wg.Wait()
	
	// Should have some successes and some rate limit errors
	s.Require().True(successCount > 0, "Should have some successful requests")
	s.Require().True(errorCount > 0, "Should have some rate limited requests")
	s.Require().Equal(int32(numGoroutines*requestsPerGoroutine), successCount+errorCount)

	_ = testSvc
}

func (s *RateLimitInterceptorTestSuite) TestRateLimitInterceptor_MaxKeys() {
	logger := logtest.NewRecorder()
	rate := Rate{Count: 1, Duration: time.Second}
	
	// Custom key function that generates different keys
	keyCounter := 0
	customKeyFunc := func(ctx context.Context, fullMethod string) (string, bool, error) {
		keyCounter++
		return fmt.Sprintf("key-%d", keyCounter), false, nil
	}
	
	options := []RateLimitOption{
		WithRateLimitGetKey(customKeyFunc),
		WithRateLimitMaxKeys(2), // Limit to 2 keys
	}
	
	testSvc, client, cleanup, err := s.setupTestServiceWithRateLimit(logger, rate, options)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(cleanup()) }()

	// Each request uses a different key, so all should succeed initially
	for i := 0; i < 3; i++ {
		_, err := client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		s.Require().NoError(err, "Request %d should succeed with different key", i+1)
	}

	_ = testSvc
}

// Helper methods

func (s *RateLimitInterceptorTestSuite) setupTestServiceWithRateLimit(
	logger *logtest.Recorder, 
	rate Rate, 
	options []RateLimitOption,
) (*testService, grpc_testing.TestServiceClient, func() error, error) {
	unaryInterceptor := RateLimitUnaryInterceptor(rate, options...)
	
	serverOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			unaryInterceptor,
			LoggingUnaryInterceptor(logger),
		),
	}
	
	return startTestService(serverOptions, nil)
}

func (s *RateLimitInterceptorTestSuite) setupTestServiceWithRateLimitStream(
	logger *logtest.Recorder, 
	rate Rate, 
	options []RateLimitOption,
) (*testService, grpc_testing.TestServiceClient, func() error, error) {
	streamInterceptor := RateLimitStreamInterceptor(rate, options...)
	
	serverOptions := []grpc.ServerOption{
		grpc.ChainStreamInterceptor(
			streamInterceptor,
			LoggingStreamInterceptor(logger),
		),
	}
	
	return startTestService(serverOptions, nil)
}