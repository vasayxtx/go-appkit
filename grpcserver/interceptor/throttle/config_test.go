/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package throttle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/acronis/go-appkit/config"
)

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				RateLimitZones: map[string]RateLimitZoneConfig{
					"zone1": {
						RateLimit: RateLimitValue{Count: 10, Duration: time.Second},
						ServiceConfig: ServiceConfig{
							Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
						},
					},
				},
				Rules: []RuleConfig{
					{
						ServiceMethods: []string{"/test.Service/*"},
						RateLimits:     []RuleRateLimit{{Zone: "zone1"}},
					},
				},
			},
			expectErr: false,
		},
		{
			name: "invalid rate limit zone - zero rate",
			config: &Config{
				RateLimitZones: map[string]RateLimitZoneConfig{
					"zone1": {
						RateLimit: RateLimitValue{Count: 0, Duration: time.Second},
						ServiceConfig: ServiceConfig{
							Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
						},
					},
				},
				Rules: []RuleConfig{
					{
						ServiceMethods: []string{"/test.Service/*"},
						RateLimits:     []RuleRateLimit{{Zone: "zone1"}},
					},
				},
			},
			expectErr: true,
		},
		{
			name: "invalid in-flight limit zone - zero limit",
			config: &Config{
				InFlightLimitZones: map[string]InFlightLimitZoneConfig{
					"zone1": {
						InFlightLimit: 0,
						ServiceConfig: ServiceConfig{
							Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
						},
					},
				},
				Rules: []RuleConfig{
					{
						ServiceMethods:  []string{"/test.Service/*"},
						InFlightLimits: []RuleInFlightLimit{{Zone: "zone1"}},
					},
				},
			},
			expectErr: true,
		},
		{
			name: "rule references undefined zone",
			config: &Config{
				Rules: []RuleConfig{
					{
						ServiceMethods: []string{"/test.Service/*"},
						RateLimits:     []RuleRateLimit{{Zone: "undefined_zone"}},
					},
				},
			},
			expectErr: true,
		},
		{
			name: "rule with no service methods",
			config: &Config{
				RateLimitZones: map[string]RateLimitZoneConfig{
					"zone1": {
						RateLimit: RateLimitValue{Count: 10, Duration: time.Second},
						ServiceConfig: ServiceConfig{
							Key: ServiceKeyConfig{Type: ServiceKeyTypeMethod},
						},
					},
				},
				Rules: []RuleConfig{
					{
						ServiceMethods: []string{},
						RateLimits:     []RuleRateLimit{{Zone: "zone1"}},
					},
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRateLimitValue_UnmarshalText(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  RateLimitValue
		expectErr bool
	}{
		{
			name:     "valid rate per second",
			input:    "10/s",
			expected: RateLimitValue{Count: 10, Duration: time.Second},
		},
		{
			name:     "valid rate per minute",
			input:    "100/m",
			expected: RateLimitValue{Count: 100, Duration: time.Minute},
		},
		{
			name:     "valid rate per hour",
			input:    "1000/h",
			expected: RateLimitValue{Count: 1000, Duration: time.Hour},
		},
		{
			name:      "invalid format - no slash",
			input:     "10s",
			expectErr: true,
		},
		{
			name:      "invalid format - non-numeric count",
			input:     "abc/s",
			expectErr: true,
		},
		{
			name:      "invalid format - invalid duration",
			input:     "10/x",
			expectErr: true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: RateLimitValue{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rl RateLimitValue
			err := rl.UnmarshalText([]byte(tt.input))
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, rl)
			}
		})
	}
}

func TestRateLimitRetryAfterValue_UnmarshalText(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  RateLimitRetryAfterValue
		expectErr bool
	}{
		{
			name:     "auto value",
			input:    "auto",
			expected: RateLimitRetryAfterValue{IsAuto: true},
		},
		{
			name:     "duration value",
			input:    "5s",
			expected: RateLimitRetryAfterValue{Duration: 5 * time.Second},
		},
		{
			name:     "empty string",
			input:    "",
			expected: RateLimitRetryAfterValue{Duration: 0},
		},
		{
			name:      "invalid duration",
			input:     "invalid",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ra RateLimitRetryAfterValue
			err := ra.UnmarshalText([]byte(tt.input))
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, ra)
			}
		})
	}
}

func TestTagsList_UnmarshalText(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TagsList
	}{
		{
			name:     "single tag",
			input:    "tag1",
			expected: TagsList{"tag1"},
		},
		{
			name:     "multiple tags",
			input:    "tag1,tag2,tag3",
			expected: TagsList{"tag1", "tag2", "tag3"},
		},
		{
			name:     "tags with spaces",
			input:    "tag1, tag2 , tag3",
			expected: TagsList{"tag1", "tag2", "tag3"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: TagsList{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tl TagsList
			err := tl.UnmarshalText([]byte(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.expected, tl)
		})
	}
}

func TestConfig_YAMLUnmarshaling(t *testing.T) {
	yamlConfig := `
rateLimitZones:
  api_zone:
    rateLimit: 100/m
    burstLimit: 10
    alg: leaky_bucket
    key:
      type: method
    maxKeys: 1000
    dryRun: false
  user_zone:
    rateLimit: 10/s
    alg: sliding_window
    key:
      type: identity
    responseRetryAfter: auto

inFlightLimitZones:
  concurrent_zone:
    inFlightLimit: 50
    key:
      type: method
    backlogLimit: 10
    backlogTimeout: 5s
    responseRetryAfter: 3s

rules:
  - alias: api_throttling
    serviceMethods:
      - "/api.Service/*"
      - "/other.Service/Method"
    excludedServiceMethods:
      - "/api.Service/HealthCheck"
    tags: "api,throttling"
    rateLimits:
      - zone: api_zone
      - zone: user_zone
    inFlightLimits:
      - zone: concurrent_zone
`

	var cfg Config
	err := yaml.Unmarshal([]byte(yamlConfig), &cfg)
	require.NoError(t, err)

	// Validate the configuration
	err = cfg.Validate()
	require.NoError(t, err)

	// Check rate limit zones
	require.Len(t, cfg.RateLimitZones, 2)
	
	apiZone := cfg.RateLimitZones["api_zone"]
	require.Equal(t, RateLimitValue{Count: 100, Duration: time.Minute}, apiZone.RateLimit)
	require.Equal(t, 10, apiZone.BurstLimit)
	require.Equal(t, RateLimitAlgLeakyBucket, apiZone.Alg)
	require.Equal(t, ServiceKeyTypeMethod, apiZone.Key.Type)
	require.Equal(t, 1000, apiZone.MaxKeys)
	require.False(t, apiZone.DryRun)

	userZone := cfg.RateLimitZones["user_zone"]
	require.Equal(t, RateLimitValue{Count: 10, Duration: time.Second}, userZone.RateLimit)
	require.Equal(t, RateLimitAlgSlidingWindow, userZone.Alg)
	require.Equal(t, ServiceKeyTypeIdentity, userZone.Key.Type)
	require.True(t, userZone.ResponseRetryAfter.IsAuto)

	// Check in-flight limit zones
	require.Len(t, cfg.InFlightLimitZones, 1)
	
	concurrentZone := cfg.InFlightLimitZones["concurrent_zone"]
	require.Equal(t, 50, concurrentZone.InFlightLimit)
	require.Equal(t, ServiceKeyTypeMethod, concurrentZone.Key.Type)
	require.Equal(t, 10, concurrentZone.BacklogLimit)
	require.Equal(t, config.TimeDuration(5*time.Second), concurrentZone.BacklogTimeout)
	require.Equal(t, config.TimeDuration(3*time.Second), concurrentZone.ResponseRetryAfter)

	// Check rules
	require.Len(t, cfg.Rules, 1)
	
	rule := cfg.Rules[0]
	require.Equal(t, "api_throttling", rule.Alias)
	require.Equal(t, []string{"/api.Service/*", "/other.Service/Method"}, rule.ServiceMethods)
	require.Equal(t, []string{"/api.Service/HealthCheck"}, rule.ExcludedServiceMethods)
	require.Equal(t, TagsList{"api", "throttling"}, rule.Tags)
	require.Len(t, rule.RateLimits, 2)
	require.Equal(t, "api_zone", rule.RateLimits[0].Zone)
	require.Equal(t, "user_zone", rule.RateLimits[1].Zone)
	require.Len(t, rule.InFlightLimits, 1)
	require.Equal(t, "concurrent_zone", rule.InFlightLimits[0].Zone)
}

func TestRuleConfig_Name(t *testing.T) {
	tests := []struct {
		name     string
		rule     RuleConfig
		expected string
	}{
		{
			name: "with alias",
			rule: RuleConfig{
				Alias:          "test_rule",
				ServiceMethods: []string{"/test.Service/*"},
			},
			expected: "test_rule",
		},
		{
			name: "without alias - single method",
			rule: RuleConfig{
				ServiceMethods: []string{"/test.Service/Method"},
			},
			expected: "/test.Service/Method",
		},
		{
			name: "without alias - multiple methods",
			rule: RuleConfig{
				ServiceMethods: []string{"/test.Service/Method1", "/test.Service/Method2"},
			},
			expected: "/test.Service/Method1; /test.Service/Method2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.rule.Name())
		})
	}
}