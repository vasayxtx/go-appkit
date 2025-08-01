/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

// Package throttle provides configurable gRPC interceptors for rate limiting and in-flight request limiting.
//
// This package implements two main interceptors:
//   - UnaryInterceptor: For unary gRPC calls
//   - StreamInterceptor: For streaming gRPC calls
//
// Both interceptors support:
//   - Rate limiting with configurable algorithms (leaky bucket, sliding window)
//   - In-flight request limiting
//   - Service method pattern matching
//   - Identity-based and method-based key extraction
//   - Dry-run mode for testing
//   - Comprehensive configuration through JSON/YAML
//
// Example usage:
//
//	cfg := &throttle.Config{
//		RateLimitZones: map[string]throttle.RateLimitZoneConfig{
//			"api_zone": {
//				RateLimit: throttle.RateLimitValue{Count: 100, Duration: time.Minute},
//				ServiceConfig: throttle.ServiceConfig{
//					Key: throttle.ServiceKeyConfig{Type: throttle.ServiceKeyTypeMethod},
//				},
//			},
//		},
//		Rules: []throttle.RuleConfig{
//			{
//				ServiceMethods: []string{"/api.Service/*"},
//				RateLimits:     []throttle.RuleRateLimit{{Zone: "api_zone"}},
//			},
//		},
//	}
//
//	unaryInterceptor, err := throttle.UnaryInterceptor(cfg)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	streamInterceptor, err := throttle.StreamInterceptor(cfg)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	server := grpc.NewServer(
//		grpc.ChainUnaryInterceptor(unaryInterceptor),
//		grpc.ChainStreamInterceptor(streamInterceptor),
//	)
package throttle