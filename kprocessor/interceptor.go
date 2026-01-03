package kprocessor

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"
)

// ProcessorHandler is the actual processing function
type ProcessorHandler[Kin, Vin any] func(ctx context.Context, record Record[Kin, Vin]) error

// ProcessorInterceptor wraps processor execution with custom logic
// Signature matches gRPC's interceptor pattern: (ctx, req, handler) -> error
type ProcessorInterceptor[Kin, Vin any] func(
	ctx context.Context,
	record Record[Kin, Vin],
	handler ProcessorHandler[Kin, Vin],
) error

// InterceptorChain manages multiple interceptors in execution order
type InterceptorChain[Kin, Vin any] struct {
	interceptors []ProcessorInterceptor[Kin, Vin]
}

// ChainInterceptors creates a new interceptor chain
func ChainInterceptors[Kin, Vin any](interceptors ...ProcessorInterceptor[Kin, Vin]) *InterceptorChain[Kin, Vin] {
	return &InterceptorChain[Kin, Vin]{
		interceptors: interceptors,
	}
}

// Execute runs the interceptor chain followed by the final handler
// Interceptors execute outer-to-inner (first interceptor wraps all others)
func (c *InterceptorChain[Kin, Vin]) Execute(
	ctx context.Context,
	record Record[Kin, Vin],
	finalHandler ProcessorHandler[Kin, Vin],
) error {
	if len(c.interceptors) == 0 {
		return finalHandler(ctx, record)
	}

	// Build handler chain from innermost to outermost
	handler := finalHandler
	for i := len(c.interceptors) - 1; i >= 0; i-- {
		interceptor := c.interceptors[i]
		next := handler
		handler = func(ctx context.Context, record Record[Kin, Vin]) error {
			return interceptor(ctx, record, next)
		}
	}

	return handler(ctx, record)
}

// LoggingInterceptor logs before and after processing
func LoggingInterceptor[Kin, Vin any](logger *slog.Logger) ProcessorInterceptor[Kin, Vin] {
	return func(ctx context.Context, record Record[Kin, Vin], handler ProcessorHandler[Kin, Vin]) error {
		logger.Info("Processing record",
			"topic", record.Metadata.Topic,
			"partition", record.Metadata.Partition,
			"offset", record.Metadata.Offset,
		)

		err := handler(ctx, record)

		if err != nil {
			logger.Error("Processing failed", "error", err)
		} else {
			logger.Info("Processing succeeded")
		}

		return err
	}
}

// MetricsInterceptor tracks processing time and counts
func MetricsInterceptor[Kin, Vin any](recordsProcessed *atomic.Int64, processingTime *atomic.Int64) ProcessorInterceptor[Kin, Vin] {
	return func(ctx context.Context, record Record[Kin, Vin], handler ProcessorHandler[Kin, Vin]) error {
		start := time.Now()
		err := handler(ctx, record)
		duration := time.Since(start)

		recordsProcessed.Add(1)
		processingTime.Add(int64(duration))

		return err
	}
}

// TracingInterceptor propagates tracing context via headers
// Note: This is a simplified version. For production, integrate with actual tracing libraries
// like OpenTelemetry.
func TracingInterceptor[Kin, Vin any]() ProcessorInterceptor[Kin, Vin] {
	return func(ctx context.Context, record Record[Kin, Vin], handler ProcessorHandler[Kin, Vin]) error {
		// Ensure trace ID exists in headers
		if _, ok := record.Metadata.Headers.Get("trace-id"); !ok {
			// Generate new trace ID if not present
			traceID := fmt.Sprintf("trace-%d", time.Now().UnixNano())
			record.Metadata.Headers.Set("trace-id", []byte(traceID))
		}

		// In a real implementation, you would:
		// - Create a span using your tracing library
		// - Inject the span into the context
		// - Record span attributes
		// - End the span after processing

		err := handler(ctx, record)

		// Record error in span if present
		// span.RecordError(err)

		return err
	}
}

// ErrorHandlingInterceptor adds retry logic
func ErrorHandlingInterceptor[Kin, Vin any](maxRetries int, retryDelay time.Duration) ProcessorInterceptor[Kin, Vin] {
	return func(ctx context.Context, record Record[Kin, Vin], handler ProcessorHandler[Kin, Vin]) error {
		var err error
		for i := 0; i <= maxRetries; i++ {
			err = handler(ctx, record)
			if err == nil {
				return nil
			}

			if i < maxRetries {
				time.Sleep(retryDelay)
			}
		}
		return fmt.Errorf("failed after %d retries: %w", maxRetries, err)
	}
}
