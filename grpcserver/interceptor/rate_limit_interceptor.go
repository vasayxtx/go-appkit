/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"fmt"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/RussellLuo/slidingwindow"
	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/memstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/acronis/go-appkit/log"
	"github.com/acronis/go-appkit/lrucache"
)

// DefaultRateLimitMaxKeys is a default value of maximum keys number for the RateLimit interceptor.
const DefaultRateLimitMaxKeys = 10000

// DefaultRateLimitBacklogTimeout determines how long the gRPC request may be in the backlog status.
const DefaultRateLimitBacklogTimeout = time.Second * 5

// RateLimitLogFieldKey it is the name of the logged field that contains a key for the requests rate limiter.
const RateLimitLogFieldKey = "rate_limit_key"

// RateLimitAlg represents a type for specifying rate-limiting algorithm.
type RateLimitAlg int

// Supported rate-limiting algorithms.
const (
	RateLimitAlgLeakyBucket RateLimitAlg = iota
	RateLimitAlgSlidingWindow
)

// RateLimitParams contains data that relates to the rate limiting procedure
// and could be used for rejecting or handling an occurred error.
type RateLimitParams struct {
	Key                 string
	RequestBacklogged   bool
	EstimatedRetryAfter time.Duration
}

// RateLimitGetKeyFunc is a function that is called for getting key for rate limiting.
type RateLimitGetKeyFunc func(ctx context.Context, fullMethod string) (key string, bypass bool, err error)

// RateLimitOnRejectFunc is a function that is called for rejecting gRPC request when the rate limit is exceeded.
type RateLimitOnRejectFunc func(ctx context.Context, params RateLimitParams, logger log.FieldLogger) error

// RateLimitOnErrorFunc is a function that is called when an error occurs during rate limiting.
type RateLimitOnErrorFunc func(ctx context.Context, params RateLimitParams, err error, logger log.FieldLogger) error

// Rate describes the frequency of requests.
type Rate struct {
	Count    int
	Duration time.Duration
}

// RateLimitOption represents a configuration option for the rate limit interceptor.
type RateLimitOption func(*rateLimitOptions)

type rateLimitOptions struct {
	alg            RateLimitAlg
	maxBurst       int
	getKey         RateLimitGetKeyFunc
	maxKeys        int
	dryRun         bool
	backlogLimit   int
	backlogTimeout time.Duration
	onReject       RateLimitOnRejectFunc
	onError        RateLimitOnErrorFunc
}

// WithRateLimitAlg sets the rate limiting algorithm.
func WithRateLimitAlg(alg RateLimitAlg) RateLimitOption {
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

// WithRateLimitDryRun enables dry run mode where limits are checked but not enforced.
func WithRateLimitDryRun(dryRun bool) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.dryRun = dryRun
	}
}

// WithRateLimitBacklogLimit sets the backlog limit for queuing requests.
func WithRateLimitBacklogLimit(backlogLimit int) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.backlogLimit = backlogLimit
	}
}

// WithRateLimitBacklogTimeout sets the timeout for backlogged requests.
func WithRateLimitBacklogTimeout(backlogTimeout time.Duration) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.backlogTimeout = backlogTimeout
	}
}

// WithRateLimitOnReject sets the callback for handling rejected requests.
func WithRateLimitOnReject(onReject RateLimitOnRejectFunc) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.onReject = onReject
	}
}

// WithRateLimitOnError sets the callback for handling rate limiting errors.
func WithRateLimitOnError(onError RateLimitOnErrorFunc) RateLimitOption {
	return func(opts *rateLimitOptions) {
		opts.onError = onError
	}
}

// RateLimitUnaryInterceptor is a gRPC unary interceptor that limits the rate of requests.
func RateLimitUnaryInterceptor(maxRate Rate, options ...RateLimitOption) (func(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error), error) {
	rlHandler, err := newRateLimitHandler(maxRate, options...)
	if err != nil {
		return nil, err
	}

	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		err = rlHandler.handle(ctx, info.FullMethod, func(ctx context.Context) error {
			var handlerErr error
			_, handlerErr = handler(ctx, req)
			return handlerErr
		})
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}, nil
}

// RateLimitStreamInterceptor is a gRPC stream interceptor that limits the rate of requests.
func RateLimitStreamInterceptor(maxRate Rate, options ...RateLimitOption) (func(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error, error) {
	rlHandler, err := newRateLimitHandler(maxRate, options...)
	if err != nil {
		return nil, err
	}

	return func(
		srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		return rlHandler.handle(ss.Context(), info.FullMethod, func(ctx context.Context) error {
			wrappedStream := &WrappedServerStream{ServerStream: ss, Ctx: ctx}
			return handler(srv, wrappedStream)
		})
	}, nil
}

type rateLimitHandler struct {
	limiter         grpcRateLimiter
	getKey          RateLimitGetKeyFunc
	getBacklogSlots func(key string) chan struct{}
	backlogTimeout  time.Duration
	dryRun          bool
	onReject        RateLimitOnRejectFunc
	onError         RateLimitOnErrorFunc
}

func newRateLimitHandler(maxRate Rate, options ...RateLimitOption) (*rateLimitHandler, error) {
	opts := &rateLimitOptions{
		alg:            RateLimitAlgLeakyBucket,
		backlogTimeout: DefaultRateLimitBacklogTimeout,
		onReject:       DefaultRateLimitOnReject,
		onError:        DefaultRateLimitOnError,
	}

	for _, option := range options {
		option(opts)
	}

	if opts.backlogLimit < 0 {
		return nil, fmt.Errorf("backlog limit should not be negative, got %d", opts.backlogLimit)
	}
	if opts.dryRun {
		opts.backlogLimit = 0 // Backlogging should be disabled in dry-run mode to avoid blocking requests.
	}

	maxKeys := 0
	if opts.getKey != nil {
		maxKeys = opts.maxKeys
		if maxKeys == 0 {
			maxKeys = DefaultRateLimitMaxKeys
		}
	}

	var limiter grpcRateLimiter
	var err error
	switch opts.alg {
	case RateLimitAlgLeakyBucket:
		limiter, err = newGrpcLeakyBucketLimiter(maxRate, opts.maxBurst, maxKeys)
	case RateLimitAlgSlidingWindow:
		limiter, err = newGrpcSlidingWindowLimiter(maxRate, maxKeys)
	default:
		return nil, fmt.Errorf("unknown rate limit algorithm")
	}
	if err != nil {
		return nil, err
	}

	getBacklogSlots, err := makeGrpcRateLimitBacklogSlotsProvider(opts.backlogLimit, maxKeys)
	if err != nil {
		return nil, fmt.Errorf("make rate limit backlog slots provider: %w", err)
	}

	return &rateLimitHandler{
		limiter:         limiter,
		getKey:          opts.getKey,
		getBacklogSlots: getBacklogSlots,
		backlogTimeout:  opts.backlogTimeout,
		dryRun:          opts.dryRun,
		onReject:        opts.onReject,
		onError:         opts.onError,
	}, nil
}

func (h *rateLimitHandler) handle(ctx context.Context, fullMethod string, handler func(context.Context) error) error {
	logger := GetLoggerFromContext(ctx)

	var key string
	if h.getKey != nil {
		var bypass bool
		var err error
		if key, bypass, err = h.getKey(ctx, fullMethod); err != nil {
			return h.onError(ctx, h.makeParams(key, false, 0), fmt.Errorf("get key for rate limit: %w", err), logger)
		}
		if bypass { // Rate limiting is bypassed for this request.
			return handler(ctx)
		}
	}

	allow, retryAfter, err := h.limiter.Allow(ctx, key)
	if err != nil {
		return h.onError(ctx, h.makeParams(key, false, 0), fmt.Errorf("rate limit: %w", err), logger)
	}

	if allow {
		return handler(ctx)
	}

	if h.dryRun {
		if logger != nil {
			logger.Warn("rate limit exceeded, continuing in dry run mode",
				log.String(RateLimitLogFieldKey, key))
		}
		return handler(ctx)
	}

	if h.getBacklogSlots == nil { // Backlogging is disabled.
		return h.onReject(ctx, h.makeParams(key, false, retryAfter), logger)
	}

	return h.handleBacklogProcessing(ctx, key, retryAfter, handler, logger)
}

func (h *rateLimitHandler) handleBacklogProcessing(
	ctx context.Context, key string, retryAfter time.Duration,
	handler func(context.Context) error, logger log.FieldLogger,
) error {
	backlogSlots := h.getBacklogSlots(key)
	backlogged := false
	select {
	case backlogSlots <- struct{}{}:
		backlogged = true
	default:
		// There are no free slots in the backlog, reject the request immediately.
		return h.onReject(ctx, h.makeParams(key, backlogged, retryAfter), logger)
	}

	freeBacklogSlotIfNeeded := func() {
		if backlogged {
			select {
			case <-backlogSlots:
				backlogged = false
			default:
			}
		}
	}

	defer freeBacklogSlotIfNeeded()

	backlogTimeoutTimer := time.NewTimer(h.backlogTimeout)
	defer backlogTimeoutTimer.Stop()

	retryTimer := time.NewTimer(retryAfter)
	defer retryTimer.Stop()

	var allow bool
	var err error

	for {
		select {
		case <-retryTimer.C:
			// Will do another check of the rate limit.
		case <-backlogTimeoutTimer.C:
			freeBacklogSlotIfNeeded()
			return h.onReject(ctx, h.makeParams(key, backlogged, retryAfter), logger)
		case <-ctx.Done():
			freeBacklogSlotIfNeeded()
			return h.onError(ctx, h.makeParams(key, backlogged, retryAfter), ctx.Err(), logger)
		}

		if allow, retryAfter, err = h.limiter.Allow(ctx, key); err != nil {
			freeBacklogSlotIfNeeded()
			return h.onError(ctx, h.makeParams(key, backlogged, retryAfter), fmt.Errorf("rate limit: %w", err), logger)
		}

		if allow {
			freeBacklogSlotIfNeeded()
			return handler(ctx)
		}

		if !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retryTimer.Reset(retryAfter)
	}
}

func (h *rateLimitHandler) makeParams(key string, backlogged bool, estimatedRetryAfter time.Duration) RateLimitParams {
	return RateLimitParams{
		Key:                 key,
		RequestBacklogged:   backlogged,
		EstimatedRetryAfter: estimatedRetryAfter,
	}
}

// DefaultRateLimitOnReject sends gRPC error response when the rate limit is exceeded.
func DefaultRateLimitOnReject(ctx context.Context, params RateLimitParams, logger log.FieldLogger) error {
	if logger != nil {
		logger.Warn("rate limit exceeded",
			log.String(RateLimitLogFieldKey, params.Key),
			log.Bool("request_backlogged", params.RequestBacklogged),
			log.Int64("estimated_retry_after_ms", params.EstimatedRetryAfter.Milliseconds()),
		)
	}

	// Set retry after header in gRPC metadata
	retryAfterSeconds := int(math.Ceil(params.EstimatedRetryAfter.Seconds()))
	md := metadata.New(map[string]string{
		"retry-after": strconv.Itoa(retryAfterSeconds),
	})
	if err := grpc.SetHeader(ctx, md); err != nil && logger != nil {
		logger.Warn("failed to set retry-after header", log.Error(err))
	}

	return status.Error(codes.ResourceExhausted, "Too many requests")
}

// DefaultRateLimitOnError sends gRPC error response when an error occurs during rate limiting.
func DefaultRateLimitOnError(ctx context.Context, params RateLimitParams, err error, logger log.FieldLogger) error {
	if logger != nil {
		logger.Error("rate limiting error",
			log.String(RateLimitLogFieldKey, params.Key),
			log.Error(err),
		)
	}
	return status.Error(codes.Internal, "Internal server error")
}

// DefaultRateLimitGetKeyByIP extracts client IP from gRPC context for rate limiting.
func DefaultRateLimitGetKeyByIP(ctx context.Context, fullMethod string) (string, bool, error) {
	if p, ok := peer.FromContext(ctx); ok {
		if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil {
			return host, false, nil
		}
		return p.Addr.String(), false, nil
	}
	return "", true, nil // Bypass if no peer info available
}

type grpcRateLimiter interface {
	Allow(ctx context.Context, key string) (allow bool, retryAfter time.Duration, err error)
}

// grpcLeakyBucketLimiter implements GCRA (Generic Cell Rate Algorithm). It's a leaky bucket variant algorithm.
type grpcLeakyBucketLimiter struct {
	limiter *throttled.GCRARateLimiterCtx
}

func newGrpcLeakyBucketLimiter(maxRate Rate, maxBurst, maxKeys int) (*grpcLeakyBucketLimiter, error) {
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
	return &grpcLeakyBucketLimiter{gcraLimiter}, nil
}

func (l *grpcLeakyBucketLimiter) Allow(ctx context.Context, key string) (allow bool, retryAfter time.Duration, err error) {
	limited, res, err := l.limiter.RateLimitCtx(ctx, key, 1)
	if err != nil {
		return false, 0, err
	}
	return !limited, res.RetryAfter, nil
}

type grpcSlidingWindowLimiter struct {
	getLimiter func(key string) *slidingwindow.Limiter
	maxRate    Rate
}

func newGrpcSlidingWindowLimiter(maxRate Rate, maxKeys int) (*grpcSlidingWindowLimiter, error) {
	if maxKeys == 0 {
		lim, _ := slidingwindow.NewLimiter(
			maxRate.Duration, int64(maxRate.Count), func() (slidingwindow.Window, slidingwindow.StopFunc) {
				return slidingwindow.NewLocalWindow()
			})
		return &grpcSlidingWindowLimiter{
			maxRate:    maxRate,
			getLimiter: func(_ string) *slidingwindow.Limiter { return lim },
		}, nil
	}

	store, err := lrucache.New[string, *slidingwindow.Limiter](maxKeys, nil)
	if err != nil {
		return nil, fmt.Errorf("new LRU in-memory store for keys: %w", err)
	}
	return &grpcSlidingWindowLimiter{
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

func (l *grpcSlidingWindowLimiter) Allow(_ context.Context, key string) (allow bool, retryAfter time.Duration, err error) {
	if l.getLimiter(key).Allow() {
		return true, 0, nil
	}
	now := time.Now()
	retryAfter = now.Truncate(l.maxRate.Duration).Add(l.maxRate.Duration).Sub(now)
	return false, retryAfter, nil
}

func makeGrpcRateLimitBacklogSlotsProvider(backlogLimit, maxKeys int) (func(key string) chan struct{}, error) {
	if backlogLimit == 0 {
		return nil, nil
	}
	if maxKeys == 0 {
		backlogSlots := make(chan struct{}, backlogLimit)
		return func(key string) chan struct{} {
			return backlogSlots
		}, nil
	}

	keysZone, err := lrucache.New[string, chan struct{}](maxKeys, nil)
	if err != nil {
		return nil, fmt.Errorf("new LRU in-memory store for keys: %w", err)
	}
	return func(key string) chan struct{} {
		backlogSlots, _ := keysZone.GetOrAdd(key, func() chan struct{} {
			return make(chan struct{}, backlogLimit)
		})
		return backlogSlots
	}, nil
}
