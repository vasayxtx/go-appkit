/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package throttle_test

import (
	"bytes"
	"fmt"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"strconv"

	"github.com/acronis/go-appkit/config"
	"github.com/acronis/go-appkit/httpserver/middleware/throttle"
)

// Example_zoneLevelTags demonstrates how to use zone-level tags to apply different
// throttling zones on the same routes at different stages of request processing.
//
// This is useful when you want to:
// - Apply global throttling before authentication
// - Apply per-identity throttling after authentication
// - Share the same route configuration without duplication
func Example_zoneLevelTags() {
	configReader := bytes.NewReader([]byte(`
rateLimitZones:
  # Global rate limit applied to all requests before authentication
  rl_global:
    rateLimit: 10/s
    burstLimit: 20
    responseStatusCode: 503
    responseRetryAfter: 5s

  # Per-identity rate limit applied after authentication
  rl_identity:
    rateLimit: 5/s
    burstLimit: 10
    responseStatusCode: 429
    responseRetryAfter: auto
    key:
      type: "identity"

inFlightLimitZones:
  # Global in-flight limit for all requests
  ifl_global:
    inFlightLimit: 100
    backlogLimit: 200
    backlogTimeout: 30s
    responseStatusCode: 503

  # Per-identity in-flight limit
  ifl_identity:
    inFlightLimit: 10
    backlogLimit: 20
    backlogTimeout: 30s
    responseStatusCode: 429
    key:
      type: "identity"

rules:
  # Single rule with both global and identity-based throttling
  # Zone-level tags determine which zones apply at which stage
  - routes:
    - path: "/api/v1/users"
      methods: [GET, POST, PUT, DELETE]
    rateLimits:
      - zone: rl_global
        tags: early_stage  # Applied before authentication
      - zone: rl_identity
        tags: late_stage   # Applied after authentication
    inFlightLimits:
      - zone: ifl_global
        tags: early_stage
      - zone: ifl_identity
        tags: late_stage
`))

	configLoader := config.NewLoader(config.NewViperAdapter())
	cfg := &throttle.Config{}
	if err := configLoader.LoadFromReader(configReader, config.DataTypeYAML, cfg); err != nil {
		stdlog.Fatal(err)
		return
	}

	promMetrics := throttle.NewPrometheusMetrics()
	promMetrics.MustRegister()
	defer promMetrics.Unregister()

	// Early stage middleware: applies global throttling zones (before authentication)
	earlyThrottleMiddleware, err := throttle.MiddlewareWithOpts(cfg, apiErrDomain, promMetrics, throttle.MiddlewareOpts{
		Tags: []string{"early_stage"},
	})
	if err != nil {
		stdlog.Fatal(fmt.Errorf("create early throttling middleware: %w", err))
		return
	}

	// Late stage middleware: applies identity-based throttling zones (after authentication)
	lateThrottleMiddleware, err := throttle.MiddlewareWithOpts(cfg, apiErrDomain, promMetrics, throttle.MiddlewareOpts{
		Tags: []string{"late_stage"},
		GetKeyIdentity: func(r *http.Request) (key string, bypass bool, err error) {
			username, _, ok := r.BasicAuth()
			if !ok {
				return "", true, fmt.Errorf("no basic auth")
			}
			return username, false, nil
		},
	})
	if err != nil {
		stdlog.Fatal(fmt.Errorf("create late throttling middleware: %w", err))
		return
	}

	// Create test server with both middleware stages
	srv := httptest.NewServer(
		earlyThrottleMiddleware( // Stage 1: Global throttling
			http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				// Authentication would happen here in a real application
				// For this example, we assume basic auth is present

				// Stage 2: Per-identity throttling after authentication
				lateThrottleMiddleware(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
					// Handle the request
					if r.URL.Path == "/api/v1/users" {
						rw.WriteHeader(http.StatusOK)
						_, _ = rw.Write([]byte(`{"users": []}`))
						return
					}
					rw.WriteHeader(http.StatusNotFound)
				})).ServeHTTP(rw, r)
			}),
		),
	)
	defer srv.Close()

	// Make requests with basic auth
	doRequest := func(username string) int {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/v1/users", http.NoBody)
		req.SetBasicAuth(username, username+"-password")
		resp, _ := http.DefaultClient.Do(req)
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	// Both global and per-identity throttling zones are applied
	// First request by user1 - both limits allow
	statusCode1 := doRequest("user1")
	fmt.Println("[1] GET /api/v1/users (user1) " + strconv.Itoa(statusCode1))

	// Second request by user1 - still within both limits
	statusCode2 := doRequest("user1")
	fmt.Println("[2] GET /api/v1/users (user1) " + strconv.Itoa(statusCode2))

	// Request by user2 - different identity, so identity-based limit allows it
	statusCode3 := doRequest("user2")
	fmt.Println("[3] GET /api/v1/users (user2) " + strconv.Itoa(statusCode3))

	// Output:
	// [1] GET /api/v1/users (user1) 200
	// [2] GET /api/v1/users (user1) 200
	// [3] GET /api/v1/users (user2) 200
}
