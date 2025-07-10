package grpcutil

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	grpcRequestMetricsLabelService = "grpc_service"
	grpcRequestMetricsLabelMethod  = "grpc_method"
	grpcRequestMetricsLabelCode    = "grpc_code"
)

var defaultCallDurationBuckets = []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 150, 300, 600}

type PrometheusMetrics struct {
	Duration *prometheus.HistogramVec
	InFlight *prometheus.GaugeVec
}

func NewPrometheusMetrics() *PrometheusMetrics {
	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "grpc_call_duration_seconds",
			Help:    "A histogram of the gRPC calls duration.",
			Buckets: defaultCallDurationBuckets,
		},
		[]string{grpcRequestMetricsLabelService, grpcRequestMetricsLabelMethod, grpcRequestMetricsLabelCode},
	)
	inFlight := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "grpc_call_in_flight",
			Help: "Current number of in-flight gRPC calls.",
		},
		[]string{grpcRequestMetricsLabelService, grpcRequestMetricsLabelMethod},
	)
	return &PrometheusMetrics{
		Duration: duration,
		InFlight: inFlight,
	}
}

// MustRegister does registration of metrics collector in Prometheus and panics if any error occurs.
func (pm *PrometheusMetrics) MustRegister() {
	prometheus.MustRegister(
		pm.Duration,
		pm.InFlight,
	)
}

// Unregister cancels registration of metrics collector in Prometheus.
func (pm *PrometheusMetrics) Unregister() {
	prometheus.Unregister(pm.InFlight)
	prometheus.Unregister(pm.Duration)
}

func MetricsServerUnaryInterceptor(promMetrics *PrometheusMetrics) func(
	ctx context.Context,
	req interface{},
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	return func(
		ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		startTime := GetRequestStartTimeFromContext(ctx)
		if startTime.IsZero() {
			startTime = time.Now()
			ctx = NewContextWithRequestStartTime(ctx, startTime)
		}

		service, method := splitFullMethodName(info.FullMethod)

		promMetrics.InFlight.WithLabelValues(service, method).Inc()
		defer promMetrics.InFlight.WithLabelValues(service, method).Dec()

		resp, err := handler(ctx, req)
		promMetrics.Duration.WithLabelValues(
			service, method, getCodeFromError(err).String()).Observe(time.Since(startTime).Seconds())
		return resp, err
	}
}

func getCodeFromError(err error) codes.Code {
	s, ok := status.FromError(err)
	if !ok {
		s = status.FromContextError(err)
	}
	return s.Code()
}
