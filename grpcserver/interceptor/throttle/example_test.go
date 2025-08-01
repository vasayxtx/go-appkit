/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package throttle_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"

	"github.com/acronis/go-appkit/grpcserver/interceptor/throttle"
)

func ExampleUnaryInterceptor() {
	// Create throttling configuration
	cfg := &throttle.Config{
		RateLimitZones: map[string]throttle.RateLimitZoneConfig{
			"api_zone": {
				RateLimit: throttle.RateLimitValue{Count: 100, Duration: time.Minute},
				ServiceConfig: throttle.ServiceConfig{
					Key: throttle.ServiceKeyConfig{Type: throttle.ServiceKeyTypeMethod},
				},
			},
		},
		InFlightLimitZones: map[string]throttle.InFlightLimitZoneConfig{
			"concurrent_zone": {
				InFlightLimit: 50,
				ServiceConfig: throttle.ServiceConfig{
					Key: throttle.ServiceKeyConfig{Type: throttle.ServiceKeyTypeMethod},
				},
			},
		},
		Rules: []throttle.RuleConfig{
			{
				ServiceMethods: []string{"/api.Service/*"},
				RateLimits:     []throttle.RuleRateLimit{{Zone: "api_zone"}},
				InFlightLimits: []throttle.RuleInFlightLimit{{Zone: "concurrent_zone"}},
			},
		},
	}

	// Create unary interceptor
	unaryInterceptor, err := throttle.UnaryInterceptor(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Create stream interceptor
	streamInterceptor, err := throttle.StreamInterceptor(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Use with gRPC server
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInterceptor),
		grpc.ChainStreamInterceptor(streamInterceptor),
	)

	fmt.Println("gRPC server configured with throttling interceptors")
	_ = server
	// Output: gRPC server configured with throttling interceptors
}

func ExampleUnaryInterceptorWithOpts() {
	cfg := &throttle.Config{
		RateLimitZones: map[string]throttle.RateLimitZoneConfig{
			"user_zone": {
				RateLimit: throttle.RateLimitValue{Count: 10, Duration: time.Second},
				ServiceConfig: throttle.ServiceConfig{
					Key: throttle.ServiceKeyConfig{Type: throttle.ServiceKeyTypeIdentity},
				},
			},
		},
		Rules: []throttle.RuleConfig{
			{
				ServiceMethods: []string{"/user.Service/*"},
				RateLimits:     []throttle.RuleRateLimit{{Zone: "user_zone"}},
				Tags:           throttle.TagsList{"user", "api"},
			},
		},
	}

	// Custom identity key extraction
	getKeyIdentity := func(ctx context.Context, fullMethod string) (string, bool, error) {
		// Extract user ID from context metadata or JWT token
		// This is just an example - implement based on your authentication system
		userID := "user123" // Would normally extract from context
		return userID, false, nil
	}

	// Create interceptor with custom options
	unaryInterceptor, err := throttle.UnaryInterceptorWithOpts(cfg, throttle.Opts{
		GetKeyIdentity: getKeyIdentity,
		Tags:           []string{"user"}, // Only apply rules with "user" tag
	})
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(unaryInterceptor))
	fmt.Println("gRPC server configured with custom throttling options")
	_ = server
	// Output: gRPC server configured with custom throttling options
}