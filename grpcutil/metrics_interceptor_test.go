package grpcutil

import (
	"context"
	"testing"

	"github.com/acronis/go-appkit/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/status"
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
			return promMetrics.Duration.WithLabelValues("grpc.testing.TestService", "UnaryCall", code.String()).(prometheus.Histogram)
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

		gauge := promMetrics.InFlight.WithLabelValues("grpc.testing.TestService", "UnaryCall")

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
