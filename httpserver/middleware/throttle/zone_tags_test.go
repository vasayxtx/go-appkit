/*
Copyright © 2024 Acronis International GmbH.

Released under MIT license.
*/

package throttle

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/acronis/go-appkit/config"
)

func TestZoneLevelTags(t *testing.T) {
	tests := []struct {
		Name    string
		CfgData string
		Func    func(t *testing.T, cfg *Config)
	}{
		{
			Name: "zone-level tags with no rule-level tags",
			CfgData: `
rateLimitZones:
  rl_zone1:
    rateLimit: 1/m
    burstLimit: 10
    responseStatusCode: 503
    responseRetryAfter: 5s
  rl_zone2:
    rateLimit: 1/m
    burstLimit: 5
    responseStatusCode: 503
    responseRetryAfter: 5s
inFlightLimitZones:
  ifl_zone1:
    inFlightLimit: 5
    backlogLimit: 5
    backlogTimeout: 30s
    responseStatusCode: 503
    responseRetryAfter: 5s
  ifl_zone2:
    inFlightLimit: 3
    backlogLimit: 3
    backlogTimeout: 30s
    responseStatusCode: 503
    responseRetryAfter: 5s
rules:
  - routes:
    - path: "/api/test"
      methods: GET
    rateLimits:
      - zone: rl_zone1
        tags: tag_a
      - zone: rl_zone2
        tags: tag_b
    inFlightLimits:
      - zone: ifl_zone1
        tags: tag_a
      - zone: ifl_zone2
        tags: tag_b
`,
			Func: func(t *testing.T, cfg *Config) {
				reqsGen := makeReqsGenerator([]string{"GET /api/test"})

				// Tag_a: Only rl_zone1 (burst 10) and ifl_zone1 should apply
				checkRateLimiting(t, cfg, reqsGen, 11, 30, 503, time.Second*5, "tag_a")

				// Tag_b: Only rl_zone2 (burst 5) and ifl_zone2 should apply
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5, "tag_b")

				// Tag_c: No matching zones
				checkNoRateLimiting(t, cfg, reqsGen, 30, "tag_c")

				// No tags: No zones should apply (no rule-level tags)
				checkNoRateLimiting(t, cfg, reqsGen, 30)
			},
		},
		{
			Name: "zone-level tags with rule-level tags - rule takes precedence",
			CfgData: `
rateLimitZones:
  rl_zone1:
    rateLimit: 1/m
    burstLimit: 10
    responseStatusCode: 503
    responseRetryAfter: 5s
  rl_zone2:
    rateLimit: 1/m
    burstLimit: 5
    responseStatusCode: 503
    responseRetryAfter: 5s
rules:
  - routes:
    - path: "/api/test"
      methods: GET
    rateLimits:
      - zone: rl_zone1
        tags: tag_zone_a
      - zone: rl_zone2
        tags: tag_zone_b
    tags: tag_rule
`,
			Func: func(t *testing.T, cfg *Config) {
				reqsGen := makeReqsGenerator([]string{"GET /api/test"})

				// Rule-level tag matches: All zones should apply (both rl_zone1 and rl_zone2)
				// Since both zones apply, the most restrictive one (rl_zone2 with burst 5) takes effect
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5, "tag_rule")

				// Zone-level tag for zone1: Only that zone applies (burst 10)
				checkRateLimiting(t, cfg, reqsGen, 11, 30, 503, time.Second*5, "tag_zone_a")

				// Zone-level tag for zone2: Only that zone applies (burst 5)
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5, "tag_zone_b")

				// Both zone tags: Both zones apply
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5, "tag_zone_a", "tag_zone_b")

				// No matching tags
				checkNoRateLimiting(t, cfg, reqsGen, 30, "tag_other")
			},
		},
		{
			Name: "zone-level tags mixed with zones without tags",
			CfgData: `
rateLimitZones:
  rl_zone1:
    rateLimit: 1/m
    burstLimit: 10
    responseStatusCode: 503
    responseRetryAfter: 5s
  rl_zone2:
    rateLimit: 1/m
    burstLimit: 5
    responseStatusCode: 503
    responseRetryAfter: 5s
rules:
  - routes:
    - path: "/api/test"
      methods: GET
    rateLimits:
      - zone: rl_zone1
        tags: tag_a
      - zone: rl_zone2
`,
			Func: func(t *testing.T, cfg *Config) {
				reqsGen := makeReqsGenerator([]string{"GET /api/test"})

				// Tag_a: Only rl_zone1 applies (burst 10)
				checkRateLimiting(t, cfg, reqsGen, 11, 30, 503, time.Second*5, "tag_a")

				// No tags from middleware: rl_zone2 (no tags) applies (burst 5)
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5)

				// Tag_b: No match with zone1, but zone2 has no tags so it won't apply
				// when middleware requests specific tags
				checkNoRateLimiting(t, cfg, reqsGen, 30, "tag_b")
			},
		},
		{
			Name: "in-flight zone-level tags",
			CfgData: `
inFlightLimitZones:
  ifl_zone1:
    inFlightLimit: 5
    backlogLimit: 5
    backlogTimeout: 30s
    responseStatusCode: 503
    responseRetryAfter: 5s
  ifl_zone2:
    inFlightLimit: 10
    backlogLimit: 10
    backlogTimeout: 30s
    responseStatusCode: 503
    responseRetryAfter: 5s
rules:
  - routes:
    - path: "/api/test"
      methods: GET
    inFlightLimits:
      - zone: ifl_zone1
        tags: tag_strict
      - zone: ifl_zone2
        tags: tag_relaxed
`,
			Func: func(t *testing.T, cfg *Config) {
				reqsGen := makeReqsGenerator([]string{"GET /api/test"})

				// Tag_strict: Only ifl_zone1 applies (limit 5+5=10)
				checkInFlightLimiting(t, cfg, checkInFlightLimitingParams{
					reqsGen:        reqsGen,
					totalLimit:     10,
					reqsNum:        20,
					unblockDelay:   time.Second,
					wantRespCode:   503,
					wantRetryAfter: time.Second * 5,
					tags:           []string{"tag_strict"},
				})

				// Tag_relaxed: Only ifl_zone2 applies (limit 10+10=20)
				checkInFlightLimiting(t, cfg, checkInFlightLimitingParams{
					reqsGen:        reqsGen,
					totalLimit:     20,
					reqsNum:        30,
					unblockDelay:   time.Second,
					wantRespCode:   503,
					wantRetryAfter: time.Second * 5,
					tags:           []string{"tag_relaxed"},
				})

				// No matching tags
				checkNoInFlightLimiting(t, cfg, reqsGen, 20, time.Second, "tag_other")
			},
		},
		{
			Name: "complex scenario with multiple tags",
			CfgData: `
rateLimitZones:
  rl_zone1:
    rateLimit: 1/m
    burstLimit: 20
    responseStatusCode: 503
    responseRetryAfter: 5s
  rl_zone2:
    rateLimit: 1/m
    burstLimit: 10
    responseStatusCode: 503
    responseRetryAfter: 5s
  rl_zone3:
    rateLimit: 1/m
    burstLimit: 5
    responseStatusCode: 503
    responseRetryAfter: 5s
rules:
  - routes:
    - path: "/api/test"
      methods: GET
    rateLimits:
      - zone: rl_zone1
        tags: public, anonymous
      - zone: rl_zone2
        tags: authenticated
      - zone: rl_zone3
        tags: authenticated, admin
`,
			Func: func(t *testing.T, cfg *Config) {
				reqsGen := makeReqsGenerator([]string{"GET /api/test"})

				// Public tag: Only rl_zone1 applies (burst 20)
				checkRateLimiting(t, cfg, reqsGen, 21, 30, 503, time.Second*5, "public")

				// Anonymous tag: Only rl_zone1 applies (burst 20)
				checkRateLimiting(t, cfg, reqsGen, 21, 30, 503, time.Second*5, "anonymous")

				// Authenticated tag: Both rl_zone2 and rl_zone3 apply, most restrictive wins (burst 5)
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5, "authenticated")

				// Admin tag: Only rl_zone3 applies (burst 5)
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5, "admin")

				// Multiple tags (public + authenticated): rl_zone1, rl_zone2, rl_zone3 all apply
				checkRateLimiting(t, cfg, reqsGen, 6, 30, 503, time.Second*5, "public", "authenticated")

				// No tags from middleware: No zones apply
				checkNoRateLimiting(t, cfg, reqsGen, 30)
			},
		},
		{
			Name: "rule-level and zone-level tags interaction",
			CfgData: `
rateLimitZones:
  rl_global:
    rateLimit: 1/m
    burstLimit: 100
    responseStatusCode: 503
    responseRetryAfter: 5s
  rl_identity:
    rateLimit: 1/m
    burstLimit: 10
    responseStatusCode: 429
    responseRetryAfter: 10s
rules:
  - routes:
    - path: "/api/public"
      methods: GET
    rateLimits:
      - zone: rl_global
    tags: all_requests
  - routes:
    - path: "/api/protected"
      methods: GET
    rateLimits:
      - zone: rl_global
        tags: all_requests
      - zone: rl_identity
        tags: authenticated
    tags: protected_endpoints
`,
			Func: func(t *testing.T, cfg *Config) {
				publicReqs := makeReqsGenerator([]string{"GET /api/public"})
				protectedReqs := makeReqsGenerator([]string{"GET /api/protected"})

				// Public endpoint with all_requests tag
				checkRateLimiting(t, cfg, publicReqs, 101, 150, 503, time.Second*5, "all_requests")

				// Protected endpoint with protected_endpoints tag (rule-level match): all zones apply
				// Most restrictive is rl_identity (burst 10), but it uses 429 status code
				checkRateLimiting(t, cfg, protectedReqs, 11, 30, 429, time.Second*10, "protected_endpoints")

				// Protected endpoint with all_requests tag (zone-level match): only rl_global applies
				checkRateLimiting(t, cfg, protectedReqs, 101, 150, 503, time.Second*5, "all_requests")

				// Protected endpoint with authenticated tag (zone-level match): only rl_identity applies
				checkRateLimiting(t, cfg, protectedReqs, 11, 30, 429, time.Second*10, "authenticated")

				// Protected endpoint with both zone tags: both zones apply
				checkRateLimiting(t, cfg, protectedReqs, 11, 30, 429, time.Second*10, "all_requests", "authenticated")

				// Public endpoint with other tag: no match
				checkNoRateLimiting(t, cfg, publicReqs, 30, "other_tag")
			},
		},
	}

	configLoader := config.NewLoader(config.NewViperAdapter())
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			cfg := &Config{}
			err := configLoader.LoadFromReader(bytes.NewReader([]byte(tt.CfgData)), config.DataTypeYAML, cfg)
			require.NoError(t, err)
			tt.Func(t, cfg)
		})
	}
}
