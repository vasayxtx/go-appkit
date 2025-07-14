/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package grpcserver

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/acronis/go-appkit/config"
)

func TestConfig_KeyPrefix(t *testing.T) {
	tests := []struct {
		name   string
		cfg    *Config
		expect string
	}{
		{
			name:   "default key prefix",
			cfg:    &Config{},
			expect: cfgDefaultKeyPrefix,
		},
		{
			name:   "custom key prefix",
			cfg:    &Config{keyPrefix: "custom.grpc"},
			expect: "custom.grpc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expect, tt.cfg.KeyPrefix())
		})
	}
}

func TestNewConfig(t *testing.T) {
	t.Run("default config", func(t *testing.T) {
		cfg := NewConfig()
		require.Equal(t, cfgDefaultKeyPrefix, cfg.KeyPrefix())
		require.Empty(t, cfg.Address)
	})

	t.Run("config with key prefix", func(t *testing.T) {
		cfg := NewConfig(WithKeyPrefix("custom.grpc"))
		require.Equal(t, "custom.grpc", cfg.KeyPrefix())
	})
}

func TestNewDefaultConfig(t *testing.T) {
	cfg := NewDefaultConfig()

	require.Equal(t, cfgDefaultKeyPrefix, cfg.KeyPrefix())
	require.Equal(t, defaultServerAddress, cfg.Address)
	require.Equal(t, config.TimeDuration(defaultServerShutdownTimeout), cfg.Timeouts.Shutdown)
	require.Equal(t, config.TimeDuration(defaultServerKeepaliveTime), cfg.Keepalive.Time)
	require.Equal(t, config.TimeDuration(defaultServerKeepaliveTimeout), cfg.Keepalive.Timeout)
	require.Equal(t, config.TimeDuration(defaultSlowCallThreshold), cfg.Log.SlowCallThreshold)
}

func TestConfig_SetProviderDefaults(t *testing.T) {
	cfg := NewConfig()
	va := config.NewViperAdapter()
	dp := config.NewKeyPrefixedDataProvider(va, cfg.KeyPrefix())

	cfg.SetProviderDefaults(dp)

	// Check that defaults are set (we can't easily verify through viper,
	// so we test by creating a config and seeing if values load correctly)
	testCfg := NewConfig()
	err := testCfg.Set(dp)
	require.NoError(t, err)

	require.Equal(t, defaultServerAddress, testCfg.Address)
	require.Equal(t, config.TimeDuration(defaultServerShutdownTimeout), testCfg.Timeouts.Shutdown)
}

func TestConfig_Set(t *testing.T) {
	tests := []struct {
		name        string
		setupViper  func(*viper.Viper)
		expectedCfg func() *Config
		expectError bool
	}{
		{
			name: "all values set",
			setupViper: func(v *viper.Viper) {
				v.Set("grpcServer.address", ":8080")
				v.Set("grpcServer.unixSocketPath", "/tmp/grpc.sock")
				v.Set("grpcServer.tls.enabled", true)
				v.Set("grpcServer.tls.cert", "/path/to/cert")
				v.Set("grpcServer.tls.key", "/path/to/key")
				v.Set("grpcServer.timeouts.shutdown", "10s")
				v.Set("grpcServer.keepalive.time", "3m")
				v.Set("grpcServer.keepalive.timeout", "30s")
				v.Set("grpcServer.limits.maxConcurrentStreams", 100)
				v.Set("grpcServer.limits.maxRecvMessageSize", "8MB")
				v.Set("grpcServer.limits.maxSendMessageSize", "8MB")
				v.Set("grpcServer.log.callStart", true)
				v.Set("grpcServer.log.excludedMethods", []string{"/grpc.health.v1.Health/Check"})
				v.Set("grpcServer.log.slowCallThreshold", "2s")
			},
			expectedCfg: func() *Config {
				return &Config{
					Address:        ":8080",
					UnixSocketPath: "/tmp/grpc.sock",
					TLS: TLSConfig{
						Enabled:     true,
						Certificate: "/path/to/cert",
						Key:         "/path/to/key",
					},
					Timeouts: TimeoutsConfig{
						Shutdown:   config.TimeDuration(10 * time.Second),
					},
					Keepalive: KeepaliveConfig{
						Time:    config.TimeDuration(3 * time.Minute),
						Timeout: config.TimeDuration(30 * time.Second),
					},
					Limits: LimitsConfig{
						MaxConcurrentStreams: 100,
						MaxRecvMessageSize:   config.ByteSize(8 * 1024 * 1024),
						MaxSendMessageSize:   config.ByteSize(8 * 1024 * 1024),
					},
					Log: LogConfig{
						CallStart:         true,
						ExcludedMethods:   []string{"/grpc.health.v1.Health/Check"},
						SlowCallThreshold: config.TimeDuration(2 * time.Second),
					},
				}
			},
		},
		{
			name: "negative max concurrent streams",
			setupViper: func(v *viper.Viper) {
				v.Set("grpcServer.limits.maxConcurrentStreams", -1)
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			v := viper.New()
			va := config.NewViperAdapter()
			dp := config.NewKeyPrefixedDataProvider(va, cfg.KeyPrefix())
			cfg.SetProviderDefaults(dp)
			tt.setupViper(v)

			// Set up the values from viper into our data provider
			for _, key := range v.AllKeys() {
				va.Set(key, v.Get(key))
			}

			err := cfg.Set(dp)

			if tt.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.expectedCfg != nil {
				expected := tt.expectedCfg()
				expected.keyPrefix = cfg.keyPrefix
				require.Equal(t, expected, cfg)
			}
		})
	}
}

func TestTimeoutsConfig_Set(t *testing.T) {
	t.Run("valid timeouts", func(t *testing.T) {
		cfg := &TimeoutsConfig{}
		va := config.NewViperAdapter()
		va.Set("timeouts.shutdown", "10s")

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.NoError(t, err)
		require.Equal(t, config.TimeDuration(10*time.Second), cfg.Shutdown)
	})
}

func TestKeepaliveConfig_Set(t *testing.T) {
	t.Run("all values set", func(t *testing.T) {
		cfg := &KeepaliveConfig{}
		va := config.NewViperAdapter()
		va.Set("keepalive.time", "3m")
		va.Set("keepalive.timeout", "30s")

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.NoError(t, err)
		require.Equal(t, config.TimeDuration(3*time.Minute), cfg.Time)
		require.Equal(t, config.TimeDuration(30*time.Second), cfg.Timeout)
	})
}

func TestLimitsConfig_Set(t *testing.T) {
	t.Run("all values set", func(t *testing.T) {
		cfg := &LimitsConfig{}
		va := config.NewViperAdapter()
		va.Set("limits.maxConcurrentStreams", 100)
		va.Set("limits.maxRecvMessageSize", "8MB")
		va.Set("limits.maxSendMessageSize", "6MB")

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.NoError(t, err)
		require.Equal(t, uint32(100), cfg.MaxConcurrentStreams)
		require.Equal(t, config.ByteSize(8*1024*1024), cfg.MaxRecvMessageSize)
		require.Equal(t, config.ByteSize(6*1024*1024), cfg.MaxSendMessageSize)
	})

t.Run("negative max concurrent streams", func(t *testing.T) {
		cfg := &LimitsConfig{}
		va := config.NewViperAdapter()
		va.Set("limits.maxConcurrentStreams", -1)

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot be negative")
	})

	t.Run("optional max concurrent streams not set", func(t *testing.T) {
		cfg := &LimitsConfig{}
		va := config.NewViperAdapter()
		va.Set("limits.maxRecvMessageSize", "4MB")
		va.Set("limits.maxSendMessageSize", "4MB")

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.NoError(t, err)
		require.Equal(t, uint32(0), cfg.MaxConcurrentStreams)
		require.Equal(t, config.ByteSize(4*1024*1024), cfg.MaxRecvMessageSize)
		require.Equal(t, config.ByteSize(4*1024*1024), cfg.MaxSendMessageSize)
	})
}

func TestLogConfig_Set(t *testing.T) {
	t.Run("all values set", func(t *testing.T) {
		cfg := &LogConfig{}
		va := config.NewViperAdapter()
		va.Set("log.callStart", true)
		va.Set("log.excludedMethods", []string{"/grpc.health.v1.Health/Check", "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"})
		va.Set("log.slowCallThreshold", "2s")

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.NoError(t, err)
		require.True(t, cfg.CallStart)
		require.Equal(t, []string{"/grpc.health.v1.Health/Check", "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"}, cfg.ExcludedMethods)
		require.Equal(t, config.TimeDuration(2*time.Second), cfg.SlowCallThreshold)
	})
}

func TestTLSConfig_Set(t *testing.T) {
	t.Run("TLS enabled with certificates", func(t *testing.T) {
		cfg := &TLSConfig{}
		va := config.NewViperAdapter()
		va.Set("tls.enabled", true)
		va.Set("tls.cert", "/path/to/cert.pem")
		va.Set("tls.key", "/path/to/key.pem")

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.NoError(t, err)
		require.True(t, cfg.Enabled)
		require.Equal(t, "/path/to/cert.pem", cfg.Certificate)
		require.Equal(t, "/path/to/key.pem", cfg.Key)
	})

	t.Run("TLS disabled", func(t *testing.T) {
		cfg := &TLSConfig{}
		va := config.NewViperAdapter()
		va.Set("tls.enabled", false)

		dp := config.NewKeyPrefixedDataProvider(va, "")
		err := cfg.Set(dp)

		require.NoError(t, err)
		require.False(t, cfg.Enabled)
		require.Empty(t, cfg.Certificate)
		require.Empty(t, cfg.Key)
	})
}

func TestWithKeyPrefix(t *testing.T) {
	option := WithKeyPrefix("custom.grpc")
	opts := configOptions{}
	option(&opts)

	require.Equal(t, "custom.grpc", opts.keyPrefix)
}
