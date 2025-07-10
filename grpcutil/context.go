package grpcutil

import (
	"context"
	"time"

	"github.com/acronis/go-appkit/log"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyInternalRequestID
	ctxKeyLogger
	ctxKeyRequestStartTime
	ctxKeyAccessToken
)

// NewContextWithRequestID creates a new context with external request id.
func NewContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, requestID)
}

// GetRequestIDFromContext extracts external request id from the context.
func GetRequestIDFromContext(ctx context.Context) string {
	return getStringFromContext(ctx, ctxKeyRequestID)
}

// NewContextWithInternalRequestID creates a new context with internal request id.
func NewContextWithInternalRequestID(ctx context.Context, internalRequestID string) context.Context {
	return context.WithValue(ctx, ctxKeyInternalRequestID, internalRequestID)
}

// GetInternalRequestIDFromContext extracts internal request id from the context.
func GetInternalRequestIDFromContext(ctx context.Context) string {
	return getStringFromContext(ctx, ctxKeyInternalRequestID)
}

// NewContextWithRequestStartTime creates a new context with request start time.
func NewContextWithRequestStartTime(ctx context.Context, startTime time.Time) context.Context {
	return context.WithValue(ctx, ctxKeyRequestStartTime, startTime)
}

// GetRequestStartTimeFromContext extracts request start time from the context.
func GetRequestStartTimeFromContext(ctx context.Context) time.Time {
	startTime, _ := ctx.Value(ctxKeyRequestStartTime).(time.Time)
	return startTime
}

// NewContextWithLogger creates a new context with logger.
func NewContextWithLogger(ctx context.Context, logger log.FieldLogger) context.Context {
	return context.WithValue(ctx, ctxKeyLogger, logger)
}

// GetLoggerFromContext extracts logger from the context.
func GetLoggerFromContext(ctx context.Context) log.FieldLogger {
	value := ctx.Value(ctxKeyLogger)
	if value == nil {
		return nil
	}
	return value.(log.FieldLogger)
}

// NewContextWithAccessToken creates a new context containing incoming service access token.
func NewContextWithAccessToken(ctx context.Context, accessToken string) context.Context {
	return context.WithValue(ctx, ctxKeyAccessToken, accessToken)
}

// GetAccessTokenFromContext extracts incoming access token from the context.
func GetAccessTokenFromContext(ctx context.Context) string {
	value := ctx.Value(ctxKeyAccessToken)
	if value == nil {
		return ""
	}
	return value.(string)
}

func getStringFromContext(ctx context.Context, key ctxKey) string {
	value := ctx.Value(key)
	if value == nil {
		return ""
	}
	return value.(string)
}
