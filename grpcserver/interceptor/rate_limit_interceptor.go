/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"fmt"
	"time"

	"github.com/RussellLuo/slidingwindow"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/memstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/acronis/go-appkit/log"
	"github.com/acronis/go-appkit/lrucache"
)

const (
	// DefaultRateLimitMaxKeys is the default maximum number of keys to track for rate limiting.
	DefaultRateLimitMaxKeys = 10000
)

// RateLimitAlg represents a type for specifying rate-limiting algorithm.
type RateLimitAlg int

// Supported rate-limiting algorithms.
const (
	RateLimitAlgLeakyBucket RateLimitAlg = iota
	RateLimitAlgSlidingWindow
)

// Rate describes the frequency of requests.
type Rate struct {
	Count    int
	Duration time.Duration
}

// RateLimitParams contains data that relates to the rate limiting procedure.
type RateLimitParams struct {
	Key                 string
	EstimatedRetryAfter time.Duration
}

// RateLimitGetKeyFunc is a function that is called for getting key for rate limiting from gRPC context.
type RateLimitGetKeyFunc func(ctx context.Context, fullMethod string) (key string, bypass bool, err error)

// RateLimitOnRejectFunc is a function that is called for handling gRPC request when the rate limit is exceeded.
type RateLimitOnRejectFunc func(ctx context.Context, fullMethod string, params RateLimitParams, logger log.FieldLogger) error

// RateLimitOnErrorFunc is a function that is called for handling gRPC request when rate limiting encounters an error.
type RateLimitOnErrorFunc func(ctx context.Context, fullMethod string, params RateLimitParams, err error, logger log.FieldLogger) error

// rateLimitOptions represents options for rate limiting interceptors.
type rateLimitOptions struct {
	alg        RateLimitAlg
	maxBurst   int
	getKey     RateLimitGetKeyFunc
	maxKeys    int
	dryRun     bool
	onReject   RateLimitOnRejectFunc
	onError    RateLimitOnErrorFunc
}

// RateLimitOption is a function type for configuring rateLimitOptions.
type RateLimitOption func(*rateLimitOptions)

// WithRateLimitAlgorithm sets the rate limiting algorithm.
func WithRateLimitAlgorithm(alg RateLimitAlg) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.alg = alg
	}
}

// WithRateLimitMaxBurst sets the maximum burst size for leaky bucket algorithm.
func WithRateLimitMaxBurst(maxBurst int) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.maxBurst = maxBurst
	}
}

// WithRateLimitGetKey sets the function to extract rate limiting key from gRPC context.
func WithRateLimitGetKey(getKey RateLimitGetKeyFunc) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.getKey = getKey
	}
}

// WithRateLimitMaxKeys sets the maximum number of keys to track.
func WithRateLimitMaxKeys(maxKeys int) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.maxKeys = maxKeys
	}
}

// WithRateLimitDryRun enables dry run mode (logging only, no actual rate limiting).
func WithRateLimitDryRun(dryRun bool) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.dryRun = dryRun
	}
}

// WithRateLimitOnReject sets the function to handle rate limit rejections.
func WithRateLimitOnReject(onReject RateLimitOnRejectFunc) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.onReject = onReject
	}
}

// WithRateLimitOnError sets the function to handle rate limiting errors.
func WithRateLimitOnError(onError RateLimitOnErrorFunc) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.onError = onError
	}
}

// rateLimiter interface defines the rate limiting behavior.
type rateLimiter interface {
	Allow(ctx context.Context, key string) (allow bool, retryAfter time.Duration, err error)
}

// leakyBucketLimiter implements GCRA (Generic Cell Rate Algorithm). It's a leaky bucket variant algorithm.
type leakyBucketLimiter struct {
	limiter *throttled.GCRARateLimiterCtx
}

func newLeakyBucketLimiter(maxRate Rate, maxBurst, maxKeys int) (*leakyBucketLimiter, error) {
	gcraStore, err := memstore.NewCtx(maxKeys)
	if err != nil {
		return nil, fmt.Errorf("new in-memory store: %w", err)
	}
	reqQuota := throttled.RateQuota{
		MaxRate:  throttled.PerDuration(maxRate.Count, maxRate.Duration),
		MaxBurst: maxBurst,
	}
	gcraLimiter, err := throttled.NewGCRARateLimiterCtx(gcraStore, reqQuota)
	if err != nil {
		return nil, fmt.Errorf("new GCRA rate limiter: %w", err)
	}
	return &leakyBucketLimiter{gcraLimiter}, nil
}

func (l *leakyBucketLimiter) Allow(ctx context.Context, key string) (allow bool, retryAfter time.Duration, err error) {
	limited, res, err := l.limiter.RateLimitCtx(ctx, key, 1)
	if err != nil {
		return false, 0, err
	}
	return !limited, res.RetryAfter, nil
}

// slidingWindowLimiter implements sliding window rate limiting.
type slidingWindowLimiter struct {
	getLimiter func(key string) *slidingwindow.Limiter
	maxRate    Rate
}

func newSlidingWindowLimiter(maxRate Rate, maxKeys int) (*slidingWindowLimiter, error) {
	if maxKeys == 0 {
		lim, _ := slidingwindow.NewLimiter(
			maxRate.Duration, int64(maxRate.Count), func() (slidingwindow.Window, slidingwindow.StopFunc) {
				return slidingwindow.NewLocalWindow()
			})
		return &slidingWindowLimiter{
			maxRate:    maxRate,
			getLimiter: func(_ string) *slidingwindow.Limiter { return lim },
		}, nil
	}

	store, err := lrucache.New[string, *slidingwindow.Limiter](maxKeys, nil)
	if err != nil {
		return nil, fmt.Errorf("new LRU in-memory store for keys: %w", err)
	}
	return &slidingWindowLimiter{
		maxRate: maxRate,
		getLimiter: func(key string) *slidingwindow.Limiter {
			lim, _ := store.GetOrAdd(key, func() *slidingwindow.Limiter {
				lim, _ := slidingwindow.NewLimiter(
					maxRate.Duration, int64(maxRate.Count), func() (slidingwindow.Window, slidingwindow.StopFunc) {
						return slidingwindow.NewLocalWindow()
					})
				return lim
			})
			return lim
		},
	}, nil
}

func (l *slidingWindowLimiter) Allow(_ context.Context, key string) (allow bool, retryAfter time.Duration, err error) {
	if l.getLimiter(key).Allow() {
		return true, 0, nil
	}
	now := time.Now()
	retryAfter = now.Truncate(l.maxRate.Duration).Add(l.maxRate.Duration).Sub(now)
	return false, retryAfter, nil
}

// DefaultRateLimitGetKey returns the full method name as the rate limiting key.
func DefaultRateLimitGetKey(ctx context.Context, fullMethod string) (key string, bypass bool, err error) {
	return fullMethod, false, nil
}

// DefaultRateLimitOnReject returns a ResourceExhausted error when rate limit is exceeded.
func DefaultRateLimitOnReject(ctx context.Context, fullMethod string, params RateLimitParams, logger log.FieldLogger) error {
	if logger != nil {
		logger.Warn("gRPC request rate limited", 
			log.String("method", fullMethod),
			log.String("rate_limit_key", params.Key),
			log.Duration("retry_after", params.EstimatedRetryAfter))
	}
	return status.Error(codes.ResourceExhausted, "rate limit exceeded")
}

// DefaultRateLimitOnError returns an Internal error when rate limiting encounters an error.
func DefaultRateLimitOnError(ctx context.Context, fullMethod string, params RateLimitParams, err error, logger log.FieldLogger) error {
	if logger != nil {
		logger.Error("gRPC rate limiting error",
			log.String("method", fullMethod),
			log.String("rate_limit_key", params.Key),
			log.Error(err))
	}
	return status.Error(codes.Internal, "rate limiting error")
}

// DefaultRateLimitOnRejectInDryRun logs the rate limit rejection but allows the request to proceed.
func DefaultRateLimitOnRejectInDryRun(ctx context.Context, fullMethod string, params RateLimitParams, logger log.FieldLogger) error {
	if logger != nil {
		logger.Info("gRPC request would be rate limited (dry run)",
			log.String("method", fullMethod),
			log.String("rate_limit_key", params.Key),
			log.Duration("retry_after", params.EstimatedRetryAfter))
	}
	return nil // Allow request to proceed in dry run mode
}

// RateLimitUnaryInterceptor is a gRPC unary interceptor that applies rate limiting to requests.
func RateLimitUnaryInterceptor(maxRate Rate, options ...RateLimitOption) func(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	opts := &rateLimitOptions{
		alg:      RateLimitAlgLeakyBucket,
		maxBurst: maxRate.Count,
		getKey:   DefaultRateLimitGetKey,
		maxKeys:  DefaultRateLimitMaxKeys,
		onReject: DefaultRateLimitOnReject,
		onError:  DefaultRateLimitOnError,
	}
	for _, option := range options {
		option(opts)
	}

	// Create the rate limiter
	var limiter rateLimiter
	var err error
	
	maxKeys := opts.maxKeys
	if opts.getKey == nil {
		maxKeys = 0 // No per-key limiting if no key function provided
	}

	switch opts.alg {
	case RateLimitAlgLeakyBucket:
		limiter, err = newLeakyBucketLimiter(maxRate, opts.maxBurst, maxKeys)
	case RateLimitAlgSlidingWindow:
		limiter, err = newSlidingWindowLimiter(maxRate, maxKeys)
	default:
		panic(fmt.Sprintf("unsupported rate limiting algorithm: %d", opts.alg))
	}
	
	if err != nil {
		panic(fmt.Sprintf("failed to create rate limiter: %v", err))
	}

	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		return rateLimitServerInterceptor(ctx, limiter, info.FullMethod, opts, func(ctx context.Context) (interface{}, error) {
			return handler(ctx, req)
		})
	}
}

// RateLimitStreamInterceptor is a gRPC stream interceptor that applies rate limiting to requests.
func RateLimitStreamInterceptor(maxRate Rate, options ...RateLimitOption) func(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	opts := &rateLimitOptions{
		alg:      RateLimitAlgLeakyBucket,
		maxBurst: maxRate.Count,
		getKey:   DefaultRateLimitGetKey,
		maxKeys:  DefaultRateLimitMaxKeys,
		onReject: DefaultRateLimitOnReject,
		onError:  DefaultRateLimitOnError,
	}
	for _, option := range options {
		option(opts)
	}

	// Create the rate limiter
	var limiter rateLimiter
	var err error
	
	maxKeys := opts.maxKeys
	if opts.getKey == nil {
		maxKeys = 0 // No per-key limiting if no key function provided
	}

	switch opts.alg {
	case RateLimitAlgLeakyBucket:
		limiter, err = newLeakyBucketLimiter(maxRate, opts.maxBurst, maxKeys)
	case RateLimitAlgSlidingWindow:
		limiter, err = newSlidingWindowLimiter(maxRate, maxKeys)
	default:
		panic(fmt.Sprintf("unsupported rate limiting algorithm: %d", opts.alg))
	}
	
	if err != nil {
		panic(fmt.Sprintf("failed to create rate limiter: %v", err))
	}

	return func(
		srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		resp, err := rateLimitServerInterceptor(ss.Context(), limiter, info.FullMethod, opts, func(ctx context.Context) (interface{}, error) {
			wrappedStream := &WrappedServerStream{ServerStream: ss, Ctx: ctx}
			return nil, handler(srv, wrappedStream)
		})
		_ = resp // resp is always nil for stream interceptors
		return err
	}
}

// rateLimitServerInterceptor contains the common rate limiting logic for both unary and stream interceptors.
func rateLimitServerInterceptor(
	ctx context.Context,
	limiter rateLimiter,
	fullMethod string,
	opts *rateLimitOptions,
	handler func(ctx context.Context) (interface{}, error),
) (interface{}, error) {
	logger := GetLoggerFromContext(ctx)

	var key string
	if opts.getKey != nil {
		var bypass bool
		var err error
		if key, bypass, err = opts.getKey(ctx, fullMethod); err != nil {
			params := RateLimitParams{Key: "", EstimatedRetryAfter: 0}
			if rateLimitErr := opts.onError(ctx, fullMethod, params, fmt.Errorf("get key for rate limit: %w", err), logger); rateLimitErr != nil {
				return nil, rateLimitErr
			}
			return handler(ctx) // Continue if onError returns nil
		}
		if bypass { // Rate limiting is bypassed for this request.
			return handler(ctx)
		}
	}

	allow, retryAfter, err := limiter.Allow(ctx, key)
	if err != nil {
		params := RateLimitParams{Key: key, EstimatedRetryAfter: 0}
		if rateLimitErr := opts.onError(ctx, fullMethod, params, fmt.Errorf("rate limit: %w", err), logger); rateLimitErr != nil {
			return nil, rateLimitErr
		}
		return handler(ctx) // Continue if onError returns nil
	}

	if allow {
		return handler(ctx)
	}

	// Rate limit exceeded
	params := RateLimitParams{Key: key, EstimatedRetryAfter: retryAfter}
	
	if opts.dryRun {
		// In dry run mode, log but don't actually reject
		DefaultRateLimitOnRejectInDryRun(ctx, fullMethod, params, logger)
		return handler(ctx)
	}

	// Rate limit exceeded, reject the request
	return nil, opts.onReject(ctx, fullMethod, params, logger)
}