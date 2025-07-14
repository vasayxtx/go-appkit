/*
Copyright © 2025 Acronis International GmbH.

Released under MIT license.
*/

package interceptor

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"

	"github.com/acronis/go-appkit/testutil"
)

func TestMetricsServerUnaryInterceptor(t *testing.T) {
	t.Run("test histogram of the gRPC calls", func(t *testing.T) {
		const okCalls = 10
		const permissionDeniedCalls = 5

		promMetrics := NewPrometheusMetrics()

		svc, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.UnaryInterceptor(MetricsServerUnaryInterceptor(promMetrics))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "UnaryCall", string(CallMethodTypeUnary), code.String()).(prometheus.Histogram)
		}

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.PermissionDenied), 0)

		for i := 0; i < okCalls; i++ {
			_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
			require.NoError(t, err)
		}
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), okCalls)
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.PermissionDenied), 0)

		permissionDeniedErr := status.Error(codes.PermissionDenied, "Permission denied")
		svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
			return nil, permissionDeniedErr
		})
		for i := 0; i < permissionDeniedCalls; i++ {
			_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
			require.ErrorIs(t, err, permissionDeniedErr)
		}
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), okCalls)
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.PermissionDenied), permissionDeniedCalls)
	})

	t.Run("test in-flight gRPC calls", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		svc, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.UnaryInterceptor(MetricsServerUnaryInterceptor(promMetrics))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		gauge := promMetrics.InFlight.WithLabelValues(
			"grpc.testing.TestService", "UnaryCall", string(CallMethodTypeUnary))

		requireSamplesCountInGauge(t, gauge, 0)

		called, done := make(chan struct{}), make(chan struct{})
		svc.SwitchUnaryCallHandler(func(ctx context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
			close(called)
			<-done
			return &grpc_testing.SimpleResponse{}, nil
		})

		callErr := make(chan error)
		go func() {
			_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
			callErr <- err
		}()

		<-called
		requireSamplesCountInGauge(t, gauge, 1)
		close(done)
		require.NoError(t, <-callErr)
		requireSamplesCountInGauge(t, gauge, 0)
	})

	t.Run("test excluded methods", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		_, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.UnaryInterceptor(MetricsServerUnaryInterceptor(promMetrics, WithMetricsExcludedMethods("/grpc.testing.TestService/UnaryCall")))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "UnaryCall", string(CallMethodTypeUnary), code.String()).(prometheus.Histogram)
		}

		_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		require.NoError(t, err)

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)
	})

	t.Run("test multiple excluded methods", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		_, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.UnaryInterceptor(MetricsServerUnaryInterceptor(promMetrics, WithMetricsExcludedMethods("/grpc.testing.TestService/UnaryCall", "/grpc.testing.TestService/OtherCall")))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "UnaryCall", string(CallMethodTypeUnary), code.String()).(prometheus.Histogram)
		}

		_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		require.NoError(t, err)

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)
	})

	t.Run("test variadic options pattern", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		_, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.UnaryInterceptor(MetricsServerUnaryInterceptor(promMetrics,
				WithMetricsExcludedMethods("/grpc.testing.TestService/ExcludedMethod"),
				WithMetricsExcludedMethods("/grpc.testing.TestService/AnotherExcludedMethod")))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "UnaryCall", string(CallMethodTypeUnary), code.String()).(prometheus.Histogram)
		}

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)

		_, err = client.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{})
		require.NoError(t, err)

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 1)
	})
}

func TestMetricsServerStreamInterceptor(t *testing.T) {
	t.Run("test histogram of the gRPC stream calls", func(t *testing.T) {
		const okCalls = 10
		const permissionDeniedCalls = 5

		promMetrics := NewPrometheusMetrics()

		svc, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.StreamInterceptor(MetricsServerStreamInterceptor(promMetrics))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "StreamingOutputCall", string(CallMethodTypeStream), code.String()).(prometheus.Histogram)
		}

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.PermissionDenied), 0)

		for i := 0; i < okCalls; i++ {
			stream, err := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
			require.NoError(t, err)
			_, err = stream.Recv()
			require.NoError(t, err)
		}
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), okCalls)
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.PermissionDenied), 0)

		permissionDeniedErr := status.Error(codes.PermissionDenied, "Permission denied")
		svc.SwitchStreamingOutputCallHandler(func(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error {
			return permissionDeniedErr
		})
		for i := 0; i < permissionDeniedCalls; i++ {
			stream, err := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
			require.NoError(t, err)
			_, err = stream.Recv()
			require.ErrorIs(t, err, permissionDeniedErr)
		}
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), okCalls)
		testutil.RequireSamplesCountInHistogram(t, getHist(codes.PermissionDenied), permissionDeniedCalls)
	})

	t.Run("test in-flight gRPC stream calls", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		svc, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.StreamInterceptor(MetricsServerStreamInterceptor(promMetrics))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		gauge := promMetrics.InFlight.WithLabelValues(
			"grpc.testing.TestService", "StreamingOutputCall", string(CallMethodTypeStream))

		requireSamplesCountInGauge(t, gauge, 0)

		called, done := make(chan struct{}), make(chan struct{})
		svc.SwitchStreamingOutputCallHandler(func(req *grpc_testing.StreamingOutputCallRequest, stream grpc_testing.TestService_StreamingOutputCallServer) error {
			close(called)
			<-done
			return stream.Send(&grpc_testing.StreamingOutputCallResponse{
				Payload: &grpc_testing.Payload{Body: []byte("test-stream")},
			})
		})

		callErr := make(chan error)
		go func() {
			stream, err := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
			if err != nil {
				callErr <- err
				return
			}
			_, err = stream.Recv()
			callErr <- err
		}()

		<-called
		requireSamplesCountInGauge(t, gauge, 1)
		close(done)
		require.NoError(t, <-callErr)
		requireSamplesCountInGauge(t, gauge, 0)
	})

	t.Run("test excluded methods", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		_, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.StreamInterceptor(MetricsServerStreamInterceptor(promMetrics, WithMetricsExcludedMethods("/grpc.testing.TestService/StreamingOutputCall")))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "StreamingOutputCall", string(CallMethodTypeStream), code.String()).(prometheus.Histogram)
		}

		stream, err := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.NoError(t, err)

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)
	})

	t.Run("test multiple excluded methods", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		_, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.StreamInterceptor(MetricsServerStreamInterceptor(promMetrics, WithMetricsExcludedMethods("/grpc.testing.TestService/StreamingOutputCall", "/grpc.testing.TestService/OtherCall")))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "StreamingOutputCall", string(CallMethodTypeStream), code.String()).(prometheus.Histogram)
		}

		stream, err := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.NoError(t, err)

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)
	})

	t.Run("test variadic options pattern", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()

		_, client, closeSvc, err := startTestService(
			[]grpc.ServerOption{grpc.StreamInterceptor(MetricsServerStreamInterceptor(promMetrics,
				WithMetricsExcludedMethods("/grpc.testing.TestService/ExcludedMethod"),
				WithMetricsExcludedMethods("/grpc.testing.TestService/AnotherExcludedMethod")))}, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, closeSvc()) }()

		getHist := func(code codes.Code) prometheus.Histogram {
			return promMetrics.Durations.WithLabelValues(
				"grpc.testing.TestService", "StreamingOutputCall", string(CallMethodTypeStream), code.String()).(prometheus.Histogram)
		}

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 0)

		stream, err := client.StreamingOutputCall(context.Background(), &grpc_testing.StreamingOutputCallRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.NoError(t, err)

		testutil.RequireSamplesCountInHistogram(t, getHist(codes.OK), 1)
	})
}

func TestNewPrometheusMetrics(t *testing.T) {
	t.Run("test default constructor", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics()
		require.NotNil(t, promMetrics)
		require.NotNil(t, promMetrics.Durations)
		require.NotNil(t, promMetrics.InFlight)
	})

	t.Run("test with namespace option", func(t *testing.T) {
		promMetrics := NewPrometheusMetrics(
			WithPrometheusNamespace("test_namespace"),
		)
		require.NotNil(t, promMetrics)
		require.NotNil(t, promMetrics.Durations)
		require.NotNil(t, promMetrics.InFlight)
	})

	t.Run("test with custom duration buckets", func(t *testing.T) {
		customBuckets := []float64{0.1, 0.5, 1.0, 5.0}
		promMetrics := NewPrometheusMetrics(
			WithPrometheusDurationBuckets(customBuckets),
		)
		require.NotNil(t, promMetrics)
		require.NotNil(t, promMetrics.Durations)
		require.NotNil(t, promMetrics.InFlight)
	})

	t.Run("test with const labels", func(t *testing.T) {
		constLabels := prometheus.Labels{"service": "test", "version": "1.0"}
		promMetrics := NewPrometheusMetrics(
			WithPrometheusConstLabels(constLabels),
		)
		require.NotNil(t, promMetrics)
		require.NotNil(t, promMetrics.Durations)
		require.NotNil(t, promMetrics.InFlight)
	})

	t.Run("test with curried label names", func(t *testing.T) {
		curriedLabels := []string{"instance", "region"}
		promMetrics := NewPrometheusMetrics(
			WithPrometheusCurriedLabelNames(curriedLabels),
		)
		require.NotNil(t, promMetrics)
		require.NotNil(t, promMetrics.Durations)
		require.NotNil(t, promMetrics.InFlight)
	})

	t.Run("test with multiple options", func(t *testing.T) {
		customBuckets := []float64{0.1, 0.5, 1.0}
		constLabels := prometheus.Labels{"service": "test"}
		promMetrics := NewPrometheusMetrics(
			WithPrometheusNamespace("multi_test"),
			WithPrometheusDurationBuckets(customBuckets),
			WithPrometheusConstLabels(constLabels),
		)
		require.NotNil(t, promMetrics)
		require.NotNil(t, promMetrics.Durations)
		require.NotNil(t, promMetrics.InFlight)
	})
}

type tHelper interface {
	Helper()
}

func assertSamplesCountInGauge(t assert.TestingT, gauge prometheus.Gauge, wantCount int) bool {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}
	reg := prometheus.NewPedanticRegistry()
	if !assert.NoError(t, reg.Register(gauge)) {
		return false
	}
	gotMetrics, err := reg.Gather()
	if !assert.NoError(t, err) {
		return false
	}
	if !assert.Equal(t, 1, len(gotMetrics)) {
		return false
	}
	return assert.Equal(t, wantCount, int(gotMetrics[0].GetMetric()[0].GetGauge().GetValue()))
}

func requireSamplesCountInGauge(t require.TestingT, gauge prometheus.Gauge, wantCount int) {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}
	if assertSamplesCountInGauge(t, gauge, wantCount) {
		return
	}
	t.FailNow()
}
