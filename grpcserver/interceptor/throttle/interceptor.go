/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package throttle

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/acronis/go-appkit/grpcserver/interceptor"
)

// Opts represents options for throttle interceptors.
type Opts struct {
	// GetKeyIdentity is a function that returns identity string representation.
	// The returned string is used as a key for zone when key.type is "identity".
	GetKeyIdentity func(ctx context.Context, fullMethod string) (key string, bypass bool, err error)

	// RateLimitOnReject is a callback called for rejecting gRPC request when the rate limit is exceeded.
	RateLimitOnReject interceptor.RateLimitUnaryOnRejectFunc

	// RateLimitOnRejectInDryRun is a callback called for rejecting gRPC request in the dry-run mode
	// when the rate limit is exceeded.
	RateLimitOnRejectInDryRun interceptor.RateLimitUnaryOnRejectFunc

	// RateLimitOnError is a callback called in case of any error that may occur during the rate limiting.
	RateLimitOnError interceptor.RateLimitUnaryOnErrorFunc

	// InFlightLimitOnReject is a callback called for rejecting gRPC request when the in-flight limit is exceeded.
	InFlightLimitOnReject interceptor.InFlightLimitUnaryOnRejectFunc

	// InFlightLimitOnRejectInDryRun is a callback called for rejecting gRPC request in the dry-run mode
	// when the in-flight limit is exceeded.
	InFlightLimitOnRejectInDryRun interceptor.InFlightLimitUnaryOnRejectFunc

	// InFlightLimitOnError is a callback called in case of any error that may occur during the in-flight limiting.
	InFlightLimitOnError interceptor.InFlightLimitUnaryOnErrorFunc

	// Tags is a list of tags for filtering throttling rules from the config. If it's empty, all rules can be applied.
	Tags []string
}

// StreamOpts represents options for throttle stream interceptors.
type StreamOpts struct {
	// GetKeyIdentity is a function that returns identity string representation.
	// The returned string is used as a key for zone when key.type is "identity".
	GetKeyIdentity func(ctx context.Context, fullMethod string) (key string, bypass bool, err error)

	// RateLimitOnReject is a callback called for rejecting gRPC request when the rate limit is exceeded.
	RateLimitOnReject interceptor.RateLimitStreamOnRejectFunc

	// RateLimitOnRejectInDryRun is a callback called for rejecting gRPC request in the dry-run mode
	// when the rate limit is exceeded.
	RateLimitOnRejectInDryRun interceptor.RateLimitStreamOnRejectFunc

	// RateLimitOnError is a callback called in case of any error that may occur during the rate limiting.
	RateLimitOnError interceptor.RateLimitStreamOnErrorFunc

	// InFlightLimitOnReject is a callback called for rejecting gRPC request when the in-flight limit is exceeded.
	InFlightLimitOnReject interceptor.InFlightLimitStreamOnRejectFunc

	// InFlightLimitOnRejectInDryRun is a callback called for rejecting gRPC request in the dry-run mode
	// when the in-flight limit is exceeded.
	InFlightLimitOnRejectInDryRun interceptor.InFlightLimitStreamOnRejectFunc

	// InFlightLimitOnError is a callback called in case of any error that may occur during the in-flight limiting.
	InFlightLimitOnError interceptor.InFlightLimitStreamOnErrorFunc

	// Tags is a list of tags for filtering throttling rules from the config. If it's empty, all rules can be applied.
	Tags []string
}

// UnaryInterceptor returns a gRPC unary interceptor that throttles requests based on the provided configuration.
func UnaryInterceptor(cfg *Config, options ...func(*Opts)) (grpc.UnaryServerInterceptor, error) {
	return UnaryInterceptorWithOpts(cfg, Opts{}, options...)
}

// UnaryInterceptorWithOpts returns a gRPC unary interceptor that throttles requests with more configurable options.
func UnaryInterceptorWithOpts(cfg *Config, opts Opts, options ...func(*Opts)) (grpc.UnaryServerInterceptor, error) {
	for _, option := range options {
		option(&opts)
	}

	interceptors, err := makeUnaryInterceptors(cfg, opts)
	if err != nil {
		return nil, err
	}

	if len(interceptors) == 0 {
		return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			return handler(ctx, req)
		}, nil
	}

	if len(interceptors) == 1 {
		return interceptors[0], nil
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		wrappedHandler := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			currentInterceptor := interceptors[i]
			currentHandler := wrappedHandler
			wrappedHandler = func(ctx context.Context, req interface{}) (interface{}, error) {
				return currentInterceptor(ctx, req, info, currentHandler)
			}
		}
		return wrappedHandler(ctx, req)
	}, nil
}

// StreamInterceptor returns a gRPC stream interceptor that throttles requests based on the provided configuration.
func StreamInterceptor(cfg *Config, options ...func(*StreamOpts)) (grpc.StreamServerInterceptor, error) {
	return StreamInterceptorWithOpts(cfg, StreamOpts{}, options...)
}

// StreamInterceptorWithOpts returns a gRPC stream interceptor that throttles requests with more configurable options.
func StreamInterceptorWithOpts(cfg *Config, opts StreamOpts, options ...func(*StreamOpts)) (grpc.StreamServerInterceptor, error) {
	for _, option := range options {
		option(&opts)
	}

	interceptors, err := makeStreamInterceptors(cfg, opts)
	if err != nil {
		return nil, err
	}

	if len(interceptors) == 0 {
		return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			return handler(srv, ss)
		}, nil
	}

	if len(interceptors) == 1 {
		return interceptors[0], nil
	}

	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		wrappedHandler := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			currentInterceptor := interceptors[i]
			currentHandler := wrappedHandler
			wrappedHandler = func(srv interface{}, ss grpc.ServerStream) error {
				return currentInterceptor(srv, ss, info, currentHandler)
			}
		}
		return wrappedHandler(srv, ss)
	}, nil
}

func makeUnaryInterceptors(cfg *Config, opts Opts) ([]grpc.UnaryServerInterceptor, error) {
	var interceptors []grpc.UnaryServerInterceptor

	for _, rule := range cfg.Rules {
		if len(rule.RateLimits) == 0 && len(rule.InFlightLimits) == 0 {
			continue
		}

		if len(opts.Tags) != 0 && !checkStringSlicesIntersect(opts.Tags, rule.Tags) {
			continue
		}

		// Build in-flight limiting interceptors
		for _, inFlightLimit := range rule.InFlightLimits {
			zoneCfg, ok := cfg.InFlightLimitZones[inFlightLimit.Zone]
			if !ok {
				return nil, fmt.Errorf("in-flight zone %q is not defined", inFlightLimit.Zone)
			}

			getKey, err := makeUnaryGetKeyFunc(zoneCfg.ServiceConfig, opts.GetKeyIdentity)
			if err != nil {
				return nil, fmt.Errorf("make get key func for in-flight zone %q: %w", inFlightLimit.Zone, err)
			}

			interceptorOpts := []interceptor.InFlightLimitOption{
				interceptor.WithInFlightLimitUnaryGetKey(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) (string, bool, error) {
					if !matchesServiceMethods(info.FullMethod, rule.ServiceMethods, rule.ExcludedServiceMethods) {
						return "", true, nil
					}
					return getKey(ctx, info.FullMethod)
				}),
				interceptor.WithInFlightLimitDryRun(zoneCfg.DryRun),
			}

			if zoneCfg.MaxKeys > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitMaxKeys(zoneCfg.MaxKeys))
			}

			if zoneCfg.BacklogLimit > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitBacklogLimit(zoneCfg.BacklogLimit))
			}

			if zoneCfg.BacklogTimeout > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitBacklogTimeout(time.Duration(zoneCfg.BacklogTimeout)))
			}

			if zoneCfg.ResponseRetryAfter > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitUnaryGetRetryAfter(
					func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) time.Duration {
						return time.Duration(zoneCfg.ResponseRetryAfter)
					},
				))
			}

			if opts.InFlightLimitOnReject != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitUnaryOnReject(opts.InFlightLimitOnReject))
			}

			if opts.InFlightLimitOnRejectInDryRun != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitUnaryOnRejectInDryRun(opts.InFlightLimitOnRejectInDryRun))
			}

			if opts.InFlightLimitOnError != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitUnaryOnError(opts.InFlightLimitOnError))
			}

			inFlightInterceptor, err := interceptor.InFlightLimitUnaryInterceptor(zoneCfg.InFlightLimit, interceptorOpts...)
			if err != nil {
				return nil, fmt.Errorf("create in-flight limit interceptor for zone %q: %w", inFlightLimit.Zone, err)
			}

			interceptors = append(interceptors, inFlightInterceptor)
		}

		// Build rate limiting interceptors  
		for _, rateLimit := range rule.RateLimits {
			zoneCfg, ok := cfg.RateLimitZones[rateLimit.Zone]
			if !ok {
				return nil, fmt.Errorf("rate limit zone %q is not defined", rateLimit.Zone)
			}

			getKey, err := makeUnaryGetKeyFunc(zoneCfg.ServiceConfig, opts.GetKeyIdentity)
			if err != nil {
				return nil, fmt.Errorf("make get key func for rate limit zone %q: %w", rateLimit.Zone, err)
			}

			var alg interceptor.RateLimitAlg
			switch zoneCfg.Alg {
			case "", RateLimitAlgLeakyBucket:
				alg = interceptor.RateLimitAlgLeakyBucket
			case RateLimitAlgSlidingWindow:
				alg = interceptor.RateLimitAlgSlidingWindow
			default:
				return nil, fmt.Errorf("unknown rate limit alg %q", zoneCfg.Alg)
			}

			interceptorOpts := []interceptor.RateLimitOption{
				interceptor.WithRateLimitAlg(alg),
				interceptor.WithRateLimitUnaryGetKey(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) (string, bool, error) {
					if !matchesServiceMethods(info.FullMethod, rule.ServiceMethods, rule.ExcludedServiceMethods) {
						return "", true, nil
					}
					return getKey(ctx, info.FullMethod)
				}),
				interceptor.WithRateLimitDryRun(zoneCfg.DryRun),
			}

			if zoneCfg.BurstLimit > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitMaxBurst(zoneCfg.BurstLimit))
			}

			if zoneCfg.MaxKeys > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitMaxKeys(zoneCfg.MaxKeys))
			}

			if zoneCfg.BacklogLimit > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitBacklogLimit(zoneCfg.BacklogLimit))
			}

			if zoneCfg.BacklogTimeout > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitBacklogTimeout(time.Duration(zoneCfg.BacklogTimeout)))
			}

			if zoneCfg.ResponseRetryAfter.Duration > 0 || zoneCfg.ResponseRetryAfter.IsAuto {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitUnaryGetRetryAfter(
					func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, estimatedTime time.Duration) time.Duration {
						if zoneCfg.ResponseRetryAfter.IsAuto {
							return estimatedTime
						}
						return zoneCfg.ResponseRetryAfter.Duration
					},
				))
			}

			if opts.RateLimitOnReject != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitUnaryOnReject(opts.RateLimitOnReject))
			}

			if opts.RateLimitOnRejectInDryRun != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitUnaryOnRejectInDryRun(opts.RateLimitOnRejectInDryRun))
			}

			if opts.RateLimitOnError != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitUnaryOnError(opts.RateLimitOnError))
			}

			rate := interceptor.Rate{Count: zoneCfg.RateLimit.Count, Duration: zoneCfg.RateLimit.Duration}
			rateLimitInterceptor, err := interceptor.RateLimitUnaryInterceptor(rate, interceptorOpts...)
			if err != nil {
				return nil, fmt.Errorf("create rate limit interceptor for zone %q: %w", rateLimit.Zone, err)
			}

			interceptors = append(interceptors, rateLimitInterceptor)
		}
	}

	return interceptors, nil
}

func makeStreamInterceptors(cfg *Config, opts StreamOpts) ([]grpc.StreamServerInterceptor, error) {
	var interceptors []grpc.StreamServerInterceptor

	for _, rule := range cfg.Rules {
		if len(rule.RateLimits) == 0 && len(rule.InFlightLimits) == 0 {
			continue
		}

		if len(opts.Tags) != 0 && !checkStringSlicesIntersect(opts.Tags, rule.Tags) {
			continue
		}

		// Build in-flight limiting interceptors
		for _, inFlightLimit := range rule.InFlightLimits {
			zoneCfg, ok := cfg.InFlightLimitZones[inFlightLimit.Zone]
			if !ok {
				return nil, fmt.Errorf("in-flight zone %q is not defined", inFlightLimit.Zone)
			}

			getKey, err := makeStreamGetKeyFunc(zoneCfg.ServiceConfig, opts.GetKeyIdentity)
			if err != nil {
				return nil, fmt.Errorf("make get key func for in-flight zone %q: %w", inFlightLimit.Zone, err)
			}

			interceptorOpts := []interceptor.InFlightLimitOption{
				interceptor.WithInFlightLimitStreamGetKey(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo) (string, bool, error) {
					if !matchesServiceMethods(info.FullMethod, rule.ServiceMethods, rule.ExcludedServiceMethods) {
						return "", true, nil
					}
					return getKey(ss.Context(), info.FullMethod)
				}),
				interceptor.WithInFlightLimitDryRun(zoneCfg.DryRun),
			}

			if zoneCfg.MaxKeys > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitMaxKeys(zoneCfg.MaxKeys))
			}

			if zoneCfg.BacklogLimit > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitBacklogLimit(zoneCfg.BacklogLimit))
			}

			if zoneCfg.BacklogTimeout > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitBacklogTimeout(time.Duration(zoneCfg.BacklogTimeout)))
			}

			if zoneCfg.ResponseRetryAfter > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitStreamGetRetryAfter(
					func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo) time.Duration {
						return time.Duration(zoneCfg.ResponseRetryAfter)
					},
				))
			}

			if opts.InFlightLimitOnReject != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitStreamOnReject(opts.InFlightLimitOnReject))
			}

			if opts.InFlightLimitOnRejectInDryRun != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitStreamOnRejectInDryRun(opts.InFlightLimitOnRejectInDryRun))
			}

			if opts.InFlightLimitOnError != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithInFlightLimitStreamOnError(opts.InFlightLimitOnError))
			}

			inFlightInterceptor, err := interceptor.InFlightLimitStreamInterceptor(zoneCfg.InFlightLimit, interceptorOpts...)
			if err != nil {
				return nil, fmt.Errorf("create in-flight limit interceptor for zone %q: %w", inFlightLimit.Zone, err)
			}

			interceptors = append(interceptors, inFlightInterceptor)
		}

		// Build rate limiting interceptors
		for _, rateLimit := range rule.RateLimits {
			zoneCfg, ok := cfg.RateLimitZones[rateLimit.Zone]
			if !ok {
				return nil, fmt.Errorf("rate limit zone %q is not defined", rateLimit.Zone)
			}

			getKey, err := makeStreamGetKeyFunc(zoneCfg.ServiceConfig, opts.GetKeyIdentity)
			if err != nil {
				return nil, fmt.Errorf("make get key func for rate limit zone %q: %w", rateLimit.Zone, err)
			}

			var alg interceptor.RateLimitAlg
			switch zoneCfg.Alg {
			case "", RateLimitAlgLeakyBucket:
				alg = interceptor.RateLimitAlgLeakyBucket
			case RateLimitAlgSlidingWindow:
				alg = interceptor.RateLimitAlgSlidingWindow
			default:
				return nil, fmt.Errorf("unknown rate limit alg %q", zoneCfg.Alg)
			}

			interceptorOpts := []interceptor.RateLimitOption{
				interceptor.WithRateLimitAlg(alg),
				interceptor.WithRateLimitStreamGetKey(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo) (string, bool, error) {
					if !matchesServiceMethods(info.FullMethod, rule.ServiceMethods, rule.ExcludedServiceMethods) {
						return "", true, nil
					}
					return getKey(ss.Context(), info.FullMethod)
				}),
				interceptor.WithRateLimitDryRun(zoneCfg.DryRun),
			}

			if zoneCfg.BurstLimit > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitMaxBurst(zoneCfg.BurstLimit))
			}

			if zoneCfg.MaxKeys > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitMaxKeys(zoneCfg.MaxKeys))
			}

			if zoneCfg.BacklogLimit > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitBacklogLimit(zoneCfg.BacklogLimit))
			}

			if zoneCfg.BacklogTimeout > 0 {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitBacklogTimeout(time.Duration(zoneCfg.BacklogTimeout)))
			}

			if zoneCfg.ResponseRetryAfter.Duration > 0 || zoneCfg.ResponseRetryAfter.IsAuto {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitStreamGetRetryAfter(
					func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, estimatedTime time.Duration) time.Duration {
						if zoneCfg.ResponseRetryAfter.IsAuto {
							return estimatedTime
						}
						return zoneCfg.ResponseRetryAfter.Duration
					},
				))
			}

			if opts.RateLimitOnReject != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitStreamOnReject(opts.RateLimitOnReject))
			}

			if opts.RateLimitOnRejectInDryRun != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitStreamOnRejectInDryRun(opts.RateLimitOnRejectInDryRun))
			}

			if opts.RateLimitOnError != nil {
				interceptorOpts = append(interceptorOpts, interceptor.WithRateLimitStreamOnError(opts.RateLimitOnError))
			}

			rate := interceptor.Rate{Count: zoneCfg.RateLimit.Count, Duration: zoneCfg.RateLimit.Duration}
			rateLimitInterceptor, err := interceptor.RateLimitStreamInterceptor(rate, interceptorOpts...)
			if err != nil {
				return nil, fmt.Errorf("create rate limit interceptor for zone %q: %w", rateLimit.Zone, err)
			}

			interceptors = append(interceptors, rateLimitInterceptor)
		}
	}

	return interceptors, nil
}

func makeUnaryGetKeyFunc(
	cfg ServiceConfig,
	getKeyIdentity func(ctx context.Context, fullMethod string) (string, bool, error),
) (func(ctx context.Context, fullMethod string) (string, bool, error), error) {
	makeByType := func() (func(ctx context.Context, fullMethod string) (string, bool, error), error) {
		switch cfg.Key.Type {
		case ServiceKeyTypeIdentity:
			if getKeyIdentity == nil {
				return nil, fmt.Errorf("GetKeyIdentity is required for identity key type")
			}
			return getKeyIdentity, nil
		case ServiceKeyTypeMethod:
			return func(ctx context.Context, fullMethod string) (string, bool, error) {
				if cfg.Key.NoBypassEmpty {
					return fullMethod, false, nil
				}
				return fullMethod, fullMethod == "", nil
			}, nil
		case ServiceKeyTypeNoKey:
			return nil, nil
		}
		return nil, fmt.Errorf("unknown key type %q", cfg.Key.Type)
	}

	getKey, err := makeByType()
	if err != nil || getKey == nil {
		return nil, err
	}

	return makeGetKeyWithPredefinedKeys(cfg.IncludedKeys, cfg.ExcludedKeys, getKey)
}

func makeStreamGetKeyFunc(
	cfg ServiceConfig,
	getKeyIdentity func(ctx context.Context, fullMethod string) (string, bool, error),
) (func(ctx context.Context, fullMethod string) (string, bool, error), error) {
	return makeUnaryGetKeyFunc(cfg, getKeyIdentity)
}

func makeGetKeyWithPredefinedKeys(
	includedKeys []string, excludedKeys []string,
	getKey func(ctx context.Context, fullMethod string) (string, bool, error),
) (func(ctx context.Context, fullMethod string) (string, bool, error), error) {
	if len(excludedKeys) == 0 && len(includedKeys) == 0 {
		return getKey, nil
	}

	if len(excludedKeys) != 0 && len(includedKeys) != 0 {
		return nil, fmt.Errorf("excluded and included keys cannot be used together")
	}

	if len(excludedKeys) != 0 {
		return func(ctx context.Context, fullMethod string) (string, bool, error) {
			key, bypass, getKeyErr := getKey(ctx, fullMethod)
			if getKeyErr != nil {
				return key, bypass, getKeyErr
			}
			if bypass {
				return key, bypass, nil
			}
			for _, excludedKey := range excludedKeys {
				if matchPattern(key, excludedKey) {
					return key, true, nil
				}
			}
			return key, false, nil
		}, nil
	}

	return func(ctx context.Context, fullMethod string) (string, bool, error) {
		key, bypass, getKeyErr := getKey(ctx, fullMethod)
		if getKeyErr != nil {
			return key, bypass, getKeyErr
		}
		if bypass {
			return key, bypass, nil
		}
		for _, includedKey := range includedKeys {
			if matchPattern(key, includedKey) {
				return key, false, nil
			}
		}
		return key, true, nil
	}, nil
}

func matchesServiceMethods(fullMethod string, serviceMethods []string, excludedServiceMethods []string) bool {
	// Check excluded methods first
	for _, excluded := range excludedServiceMethods {
		if matchPattern(fullMethod, excluded) {
			return false
		}
	}

	// Check included methods
	for _, included := range serviceMethods {
		if matchPattern(fullMethod, included) {
			return true
		}
	}

	return false
}

func matchPattern(text, pattern string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "/*") {
		prefix := strings.TrimSuffix(pattern, "/*")
		return strings.HasPrefix(text, prefix)
	}
	matched, err := path.Match(pattern, text)
	if err != nil {
		return false
	}
	return matched
}

func checkStringSlicesIntersect(slice1, slice2 []string) bool {
	for i := range slice1 {
		for j := range slice2 {
			if slice1[i] == slice2[j] {
				return true
			}
		}
	}
	return false
}