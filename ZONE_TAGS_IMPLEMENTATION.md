# Zone-Level Tags Implementation for HTTP Throttling

## Summary

This implementation adds support for zone-level tags on individual `RateLimits` and `InFlightLimits` rule items in the HTTP throttling middleware. This enhancement allows different interceptors/middlewares to apply different zones on the same routes without configuration duplication.

## Changes Made

### 1. Core Configuration Updates

#### `internal/throttleconfig/config.go`
- Added `Tags` field to `RuleRateLimit` struct
- Added `Tags` field to `RuleInFlightLimit` struct
- Both use the existing `TagsList` type for consistency

```go
type RuleRateLimit struct {
    Zone string   `mapstructure:"zone" yaml:"zone" json:"zone"`
    Tags TagsList `mapstructure:"tags" yaml:"tags" json:"tags"`
}

type RuleInFlightLimit struct {
    Zone string   `mapstructure:"zone" yaml:"zone" json:"zone"`
    Tags TagsList `mapstructure:"tags" yaml:"tags" json:"tags"`
}
```

### 2. Middleware Logic Updates

#### `httpserver/middleware/throttle/middleware.go`
- Added `shouldApplyZoneWithTags()` helper function to determine if a zone should be applied based on tag matching
- Updated `makeRoutes()` function to implement tag precedence logic:
  - **Rule-level tags take precedence**: If middleware tags match rule-level tags, ALL zones are applied
  - **Zone-level fallback**: If rule-level tags don't match (or aren't specified), check each zone's tags individually
  - **Empty tags handling**: Properly handles cases where middleware, rule, or zones have no tags

#### Tag Matching Rules:
1. **Both middleware and rule have no tags**: Check zone-level tags individually
2. **Middleware has tags, rule has no tags**: Check zone-level tags individually
3. **Middleware has no tags, rule has tags**: Skip rule entirely
4. **Both have tags and match**: Apply ALL zones regardless of zone-level tags
5. **Both have tags but don't match**: Check zone-level tags individually

#### Zone Application Rules:
- Middleware no tags + Zone no tags = **Apply**
- Middleware no tags + Zone has tags = **Don't apply**
- Middleware has tags + Zone no tags = **Don't apply**
- Middleware has tags + Zone has tags = **Apply if they intersect**

### 3. Test Coverage

#### New Test File: `httpserver/middleware/throttle/zone_tags_test.go`
Comprehensive test scenarios covering:
- Zone-level tags with no rule-level tags
- Zone-level tags with rule-level tags (precedence)
- Mixed zones (some with tags, some without)
- In-flight limiting with zone-level tags
- Complex scenarios with multiple tags
- Rule-level and zone-level tags interaction

#### Updated: `httpserver/middleware/throttle/config_test.go`
- Added `TestConfigWithZoneLevelTags` to verify proper parsing of zone-level tags from YAML/JSON
- Updated existing test assertions to handle nil vs empty `TagsList`

### 4. Documentation Updates

#### `httpserver/middleware/throttle/README.md`
- Updated Features section to highlight zone-level tags
- Added comprehensive "Zone-Level Tags Example" section with practical use cases
- Added "Tag Precedence" section explaining the precedence rules
- Added "Complex Tag Scenarios" section with advanced examples
- Updated existing "Tags" section to distinguish between rule-level and zone-level tags

#### New Example File: `httpserver/middleware/throttle/example_zone_tags_test.go`
- Demonstrates practical usage of zone-level tags
- Shows how to apply different throttling at different middleware stages
- Illustrates the benefit of avoiding configuration duplication

## Configuration Example

```yaml
rateLimitZones:
  rl_global:
    rateLimit: 1000/s
    burstLimit: 2000
    responseStatusCode: 503
  
  rl_identity:
    rateLimit: 50/s
    burstLimit: 100
    responseStatusCode: 429
    key:
      type: identity

rules:
  - routes:
    - path: "/api/v1/users"
      methods: [GET, POST, PUT, DELETE]
    rateLimits:
      - zone: rl_global
        tags: early_stage      # Applied before authentication
      - zone: rl_identity
        tags: late_stage       # Applied after authentication
```

## Usage Example

```go
// Early middleware: applies global throttling
earlyMw := MiddlewareWithOpts(cfg, "my-app", metrics, MiddlewareOpts{
    Tags: []string{"early_stage"},
})

// Late middleware: applies per-identity throttling after authentication
lateMw := MiddlewareWithOpts(cfg, "my-app", metrics, MiddlewareOpts{
    Tags: []string{"late_stage"},
    GetKeyIdentity: func(r *http.Request) (string, bool, error) {
        return getUserIDFromContext(r.Context())
    },
})
```

## Benefits

1. **No Configuration Duplication**: Same routes can be used with different zone combinations
2. **Flexible Middleware Stages**: Apply different throttling at different processing stages
3. **Backward Compatible**: Existing configurations without zone-level tags work as before
4. **Clear Precedence**: Well-defined tag precedence rules avoid ambiguity
5. **Composable**: Easy to mix global and per-identity throttling on the same endpoints

## Testing

All tests pass including:
- Unit tests for configuration parsing
- Integration tests for middleware behavior
- Race condition tests
- Example tests demonstrating usage

```bash
go test ./httpserver/middleware/throttle/...
# All tests pass
```

## Backward Compatibility

This change is fully backward compatible:
- Existing configurations without zone-level tags continue to work
- Rule-level tags behavior is unchanged when zone-level tags are not used
- No breaking changes to the API
