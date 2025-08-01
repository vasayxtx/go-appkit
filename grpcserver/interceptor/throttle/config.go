/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package throttle

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v3"

	"github.com/acronis/go-appkit/config"
)

// Rate-limiting algorithms.
const (
	RateLimitAlgLeakyBucket   = "leaky_bucket"
	RateLimitAlgSlidingWindow = "sliding_window"
)

// gRPC service key types.
const (
	ServiceKeyTypeNoKey    ServiceKeyType = ""
	ServiceKeyTypeIdentity ServiceKeyType = "identity"
	ServiceKeyTypeMethod   ServiceKeyType = "method"
)

// ServiceKeyType is a type of keys for gRPC service throttling.
type ServiceKeyType string

// Config represents a configuration for throttling of gRPC requests on the server side.
// Configuration can be loaded in different formats (YAML, JSON) using config.Loader, viper,
// or with json.Unmarshal/yaml.Unmarshal functions directly.
type Config struct {
	// RateLimitZones contains rate limiting zones.
	// Key is a zone's name, and value is a zone's configuration.
	RateLimitZones map[string]RateLimitZoneConfig `mapstructure:"rateLimitZones" yaml:"rateLimitZones" json:"rateLimitZones"`

	// InFlightLimitZones contains in-flight limiting zones.
	// Key is a zone's name, and value is a zone's configuration.
	InFlightLimitZones map[string]InFlightLimitZoneConfig `mapstructure:"inFlightLimitZones" yaml:"inFlightLimitZones" json:"inFlightLimitZones"`

	// Rules contains list of so-called throttling rules.
	// Basically, throttling rule represents a gRPC service/method pattern,
	// and rate/in-flight limiting zones based on which all matched gRPC requests will be throttled.
	Rules []RuleConfig `mapstructure:"rules" yaml:"rules" json:"rules"`

	keyPrefix string
}

var _ config.Config = (*Config)(nil)
var _ config.KeyPrefixProvider = (*Config)(nil)

// ConfigOption is a type for functional options for the Config.
type ConfigOption func(*configOptions)

type configOptions struct {
	keyPrefix string
}

// WithKeyPrefix returns a ConfigOption that sets a key prefix for parsing configuration parameters.
// This prefix will be used by config.Loader.
func WithKeyPrefix(keyPrefix string) ConfigOption {
	return func(o *configOptions) {
		o.keyPrefix = keyPrefix
	}
}

// NewConfig creates a new instance of the Config.
func NewConfig(options ...ConfigOption) *Config {
	var opts configOptions
	for _, opt := range options {
		opt(&opts)
	}
	return &Config{keyPrefix: opts.keyPrefix}
}

// KeyPrefix returns a key prefix with which all configuration parameters should be presented.
// Implements config.KeyPrefixProvider interface.
func (c *Config) KeyPrefix() string {
	return c.keyPrefix
}

// SetProviderDefaults sets default configuration values for logger in config.DataProvider.
// Implements config.Config interface.
func (c *Config) SetProviderDefaults(_ config.DataProvider) {
}

// Set sets throttling configuration values from config.DataProvider.
// Implements config.Config interface.
func (c *Config) Set(dp config.DataProvider) error {
	if err := dp.Unmarshal(c, func(decoderConfig *mapstructure.DecoderConfig) {
		decoderConfig.DecodeHook = MapstructureDecodeHook()
	}); err != nil {
		return err
	}
	return c.Validate()
}

// Validate validates configuration.
func (c *Config) Validate() error {
	for zoneName, zone := range c.RateLimitZones {
		if err := zone.Validate(); err != nil {
			return fmt.Errorf("validate rate limit zone %q: %w", zoneName, err)
		}
	}
	for zoneName, zone := range c.InFlightLimitZones {
		if err := zone.Validate(); err != nil {
			return fmt.Errorf("validate in-flight limit zone %q: %w", zoneName, err)
		}
	}
	for _, rule := range c.Rules {
		if err := rule.Validate(c.RateLimitZones, c.InFlightLimitZones); err != nil {
			return fmt.Errorf("validate rule %q: %w", rule.Name(), err)
		}
	}
	return nil
}

// ServiceConfig represents a basic service configuration.
type ServiceConfig struct {
	Key                ServiceKeyConfig `mapstructure:"key" yaml:"key" json:"key"`
	MaxKeys            int              `mapstructure:"maxKeys" yaml:"maxKeys" json:"maxKeys"`
	DryRun             bool             `mapstructure:"dryRun" yaml:"dryRun" json:"dryRun"`
	IncludedKeys       []string         `mapstructure:"includedKeys" yaml:"includedKeys" json:"includedKeys"`
	ExcludedKeys       []string         `mapstructure:"excludedKeys" yaml:"excludedKeys" json:"excludedKeys"`
}

// Validate validates service configuration.
func (c *ServiceConfig) Validate() error {
	if err := c.Key.Validate(); err != nil {
		return err
	}
	if c.MaxKeys < 0 {
		return fmt.Errorf("maximum keys should be >= 0, got %d", c.MaxKeys)
	}
	if len(c.IncludedKeys) != 0 && len(c.ExcludedKeys) != 0 {
		return fmt.Errorf("included and excluded lists cannot be specified at the same time")
	}
	return nil
}

// RateLimitZoneConfig represents zone configuration for rate limiting.
type RateLimitZoneConfig struct {
	ServiceConfig         `mapstructure:",squash" yaml:",inline"`
	Alg                   string                   `mapstructure:"alg" yaml:"alg" json:"alg"`
	RateLimit             RateLimitValue           `mapstructure:"rateLimit" yaml:"rateLimit" json:"rateLimit"`
	BurstLimit            int                      `mapstructure:"burstLimit" yaml:"burstLimit" json:"burstLimit"`
	BacklogLimit          int                      `mapstructure:"backlogLimit" yaml:"backlogLimit" json:"backlogLimit"`
	BacklogTimeout        config.TimeDuration      `mapstructure:"backlogTimeout" yaml:"backlogTimeout" json:"backlogTimeout"`
	ResponseRetryAfter    RateLimitRetryAfterValue `mapstructure:"responseRetryAfter" yaml:"responseRetryAfter" json:"responseRetryAfter"`
}

// Validate validates zone configuration for rate limiting.
func (c *RateLimitZoneConfig) Validate() error {
	if err := c.ServiceConfig.Validate(); err != nil {
		return err
	}
	if c.Alg != "" && c.Alg != RateLimitAlgLeakyBucket && c.Alg != RateLimitAlgSlidingWindow {
		return fmt.Errorf("unknown rate limit alg %q", c.Alg)
	}
	if c.RateLimit.Count < 1 {
		return fmt.Errorf("rate limit should be >= 1, got %d", c.RateLimit.Count)
	}
	if c.BurstLimit < 0 {
		return fmt.Errorf("burst limit should be >= 0, got %d", c.BurstLimit)
	}
	if c.BacklogLimit < 0 {
		return fmt.Errorf("backlog limit should be >= 0, got %d", c.BacklogLimit)
	}
	return nil
}

// InFlightLimitZoneConfig represents zone configuration for in-flight limiting.
type InFlightLimitZoneConfig struct {
	ServiceConfig      `mapstructure:",squash" yaml:",inline"`
	InFlightLimit      int                 `mapstructure:"inFlightLimit" yaml:"inFlightLimit" json:"inFlightLimit"`
	BacklogLimit       int                 `mapstructure:"backlogLimit" yaml:"backlogLimit" json:"backlogLimit"`
	BacklogTimeout     config.TimeDuration `mapstructure:"backlogTimeout" yaml:"backlogTimeout" json:"backlogTimeout"`
	ResponseRetryAfter config.TimeDuration `mapstructure:"responseRetryAfter" yaml:"responseRetryAfter" json:"responseRetryAfter"`
}

// Validate validates zone configuration for in-flight limiting.
func (c *InFlightLimitZoneConfig) Validate() error {
	if err := c.ServiceConfig.Validate(); err != nil {
		return err
	}
	if c.InFlightLimit < 1 {
		return fmt.Errorf("in-flight limit should be >= 1, got %d", c.InFlightLimit)
	}
	if c.BacklogLimit < 0 {
		return fmt.Errorf("backlog limit should be >= 0, got %d", c.BacklogLimit)
	}
	return nil
}

// ServiceKeyConfig represents a configuration of service's key.
type ServiceKeyConfig struct {
	// Type determines type of key that will be used for throttling.
	Type ServiceKeyType `mapstructure:"type" yaml:"type" json:"type"`

	// NoBypassEmpty specifies whether throttling will be used if the value obtained by the key is empty.
	NoBypassEmpty bool `mapstructure:"noBypassEmpty" yaml:"noBypassEmpty" json:"noBypassEmpty"`
}

// Validate validates service key configuration.
func (c *ServiceKeyConfig) Validate() error {
	switch c.Type {
	case ServiceKeyTypeNoKey, ServiceKeyTypeIdentity, ServiceKeyTypeMethod:
	default:
		return fmt.Errorf("unknown key service type %q", c.Type)
	}
	return nil
}

// RuleConfig represents configuration for throttling rule.
type RuleConfig struct {
	// Alias is an alternative name for the rule. It will be used as a label in metrics.
	Alias string `mapstructure:"alias" yaml:"alias" json:"alias"`

	// ServiceMethods contains a list of gRPC service methods for which the rule will be applied.
	// Patterns like "/package.Service/Method" or "/package.Service/*" are supported.
	ServiceMethods []string `mapstructure:"serviceMethods" yaml:"serviceMethods" json:"serviceMethods"`

	// ExcludedServiceMethods contains list of gRPC service methods to be excluded from throttling limitations.
	ExcludedServiceMethods []string `mapstructure:"excludedServiceMethods" yaml:"excludedServiceMethods" json:"excludedServiceMethods"`

	// Tags is useful when the different rules of the same config should be used by different interceptors.
	Tags TagsList `mapstructure:"tags" yaml:"tags" json:"tags"`

	// RateLimits contains a list of the rate limiting zones that are used in the rule.
	RateLimits []RuleRateLimit `mapstructure:"rateLimits" yaml:"rateLimits" json:"rateLimits"`

	// InFlightLimits contains a list of the in-flight limiting zones that are used in the rule.
	InFlightLimits []RuleInFlightLimit `mapstructure:"inFlightLimits" yaml:"inFlightLimits" json:"inFlightLimits"`
}

// Name returns throttling rule name.
func (c *RuleConfig) Name() string {
	if c.Alias != "" {
		return c.Alias
	}
	return strings.Join(c.ServiceMethods, "; ")
}

// Validate validates throttling rule configuration.
func (c *RuleConfig) Validate(
	rateLimitZones map[string]RateLimitZoneConfig, inFlightLimitZones map[string]InFlightLimitZoneConfig,
) error {
	for _, zone := range c.RateLimits {
		if _, ok := rateLimitZones[zone.Zone]; !ok {
			return fmt.Errorf("rate limit zone %q is undefined", zone.Zone)
		}
	}
	for _, zone := range c.InFlightLimits {
		if _, ok := inFlightLimitZones[zone.Zone]; !ok {
			return fmt.Errorf("in-flight limit zone %q is undefined", zone.Zone)
		}
	}

	if len(c.ServiceMethods) == 0 {
		return fmt.Errorf("serviceMethods is missing")
	}

	return nil
}

// RuleRateLimit represents rule's rate limiting parameters.
type RuleRateLimit struct {
	Zone string `mapstructure:"zone" yaml:"zone"`
}

// RuleInFlightLimit represents rule's in-flight limiting parameters.
type RuleInFlightLimit struct {
	Zone string `mapstructure:"zone" yaml:"zone"`
}

// RateLimitRetryAfterValue represents structured retry-after value for rate limiting.
type RateLimitRetryAfterValue struct {
	IsAuto   bool
	Duration time.Duration
}

const rateLimitRetryAfterAuto = "auto"

// String returns a string representation of the retry-after value.
// Implements fmt.Stringer interface.
func (ra RateLimitRetryAfterValue) String() string {
	if ra.IsAuto {
		return rateLimitRetryAfterAuto
	}
	return ra.Duration.String()
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (ra *RateLimitRetryAfterValue) UnmarshalText(text []byte) error {
	return ra.unmarshal(string(text))
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (ra *RateLimitRetryAfterValue) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	return ra.unmarshal(text)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (ra *RateLimitRetryAfterValue) UnmarshalYAML(value *yaml.Node) error {
	var text string
	if err := value.Decode(&text); err != nil {
		return err
	}
	return ra.unmarshal(text)
}

func (ra *RateLimitRetryAfterValue) unmarshal(retryAfterVal string) error {
	switch v := retryAfterVal; v {
	case "":
		*ra = RateLimitRetryAfterValue{Duration: 0}
	case rateLimitRetryAfterAuto:
		*ra = RateLimitRetryAfterValue{IsAuto: true}
	default:
		dur, err := time.ParseDuration(v)
		if err != nil {
			return err
		}
		*ra = RateLimitRetryAfterValue{Duration: dur}
	}
	return nil
}

// MarshalText implements the encoding.TextMarshaler interface.
func (ra RateLimitRetryAfterValue) MarshalText() ([]byte, error) {
	return []byte(ra.String()), nil
}

// MarshalJSON implements the json.Marshaler interface.
func (ra RateLimitRetryAfterValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(ra.String())
}

// MarshalYAML implements the yaml.Marshaler interface.
func (ra RateLimitRetryAfterValue) MarshalYAML() (interface{}, error) {
	return ra.String(), nil
}

// RateLimitValue represents value for rate limiting.
type RateLimitValue struct {
	Count    int
	Duration time.Duration
}

// String returns a string representation of the rate limit value.
// Implements fmt.Stringer interface.
func (rl RateLimitValue) String() string {
	if rl.Duration == 0 && rl.Count == 0 {
		return ""
	}
	var d string
	switch rl.Duration {
	case time.Second:
		d = "s"
	case time.Minute:
		d = "m"
	case time.Hour:
		d = "h"
	default:
		d = rl.Duration.String()
	}
	return fmt.Sprintf("%d/%s", rl.Count, d)
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (rl *RateLimitValue) UnmarshalText(text []byte) error {
	return rl.unmarshal(string(text))
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (rl *RateLimitValue) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	return rl.unmarshal(text)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (rl *RateLimitValue) UnmarshalYAML(value *yaml.Node) error {
	var text string
	if err := value.Decode(&text); err != nil {
		return err
	}
	return rl.unmarshal(text)
}

func (rl *RateLimitValue) unmarshal(rate string) error {
	if rate == "" {
		*rl = RateLimitValue{}
		return nil
	}
	incorrectFormatErr := fmt.Errorf(
		"incorrect format for rate %q, should be N/(s|m|h), for example 10/s, 100/m, 1000/h", rate)
	parts := strings.SplitN(rate, "/", 2)
	if len(parts) != 2 {
		return incorrectFormatErr
	}
	count, err := strconv.Atoi(parts[0])
	if err != nil {
		return incorrectFormatErr
	}
	var dur time.Duration
	switch strings.ToLower(parts[1]) {
	case "s":
		dur = time.Second
	case "m":
		dur = time.Minute
	case "h":
		dur = time.Hour
	default:
		return incorrectFormatErr
	}
	*rl = RateLimitValue{Count: count, Duration: dur}
	return nil
}

// MarshalText implements the encoding.TextMarshaler interface.
func (rl RateLimitValue) MarshalText() ([]byte, error) {
	return []byte(rl.String()), nil
}

// MarshalJSON implements the json.Marshaler interface.
func (rl RateLimitValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(rl.String())
}

// MarshalYAML implements the yaml.Marshaler interface.
func (rl RateLimitValue) MarshalYAML() (interface{}, error) {
	return rl.String(), nil
}

// TagsList represents a list of tags.
type TagsList []string

// UnmarshalText implements the encoding.TextUnmarshaler interface.
func (tl *TagsList) UnmarshalText(text []byte) error {
	tl.unmarshal(string(text))
	return nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (tl *TagsList) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		tl.unmarshal(s)
		return nil
	}
	var l []string
	if err := json.Unmarshal(data, &l); err == nil {
		*tl = l
		return nil
	}
	return fmt.Errorf("invalid methods list: %s", data)
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (tl *TagsList) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err == nil {
		tl.unmarshal(s)
		return nil
	}
	var l []string
	if err := value.Decode(&l); err == nil {
		*tl = l
		return nil
	}
	return fmt.Errorf("invalid methods list: %v", value)
}

func (tl *TagsList) unmarshal(data string) {
	data = strings.TrimSpace(data)
	if data == "" {
		*tl = TagsList{}
		return
	}
	methods := strings.Split(data, ",")
	for _, m := range methods {
		*tl = append(*tl, strings.TrimSpace(m))
	}
}

func (tl TagsList) String() string {
	return strings.Join(tl, ",")
}

// MarshalText implements the encoding.TextMarshaler interface.
func (tl TagsList) MarshalText() ([]byte, error) {
	return []byte(tl.String()), nil
}

// MarshalJSON implements the json.Marshaler interface.
func (tl TagsList) MarshalJSON() ([]byte, error) {
	return json.Marshal(tl.String())
}

// MarshalYAML implements the yaml.Marshaler interface.
func (tl TagsList) MarshalYAML() (interface{}, error) {
	return tl.String(), nil
}

func mapstructureTrimSpaceStringsHookFunc() mapstructure.DecodeHookFunc {
	return func(
		f reflect.Kind,
		t reflect.Kind,
		data interface{}) (interface{}, error) {
		if f != reflect.Slice || t != reflect.Slice {
			return data, nil
		}
		switch dt := data.(type) {
		case []string:
			res := make([]string, 0, len(dt))
			for _, s := range dt {
				res = append(res, strings.TrimSpace(s))
			}
			return res, nil
		default:
			return data, nil
		}
	}
}

// MapstructureDecodeHook returns a DecodeHookFunc for mapstructure to handle custom types.
func MapstructureDecodeHook() mapstructure.DecodeHookFunc {
	return mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.TextUnmarshallerHookFunc(),
		mapstructureTrimSpaceStringsHookFunc(),
	)
}