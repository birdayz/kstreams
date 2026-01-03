package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/birdayz/kstreams/kprocessor"
)

// OrderEvent represents an order in an e-commerce system
type OrderEvent struct {
	OrderID  string
	UserID   string
	Amount   float64
	Status   string
}

// SimpleOrderProcessor processes orders without any interceptors
// This is just the core business logic
type SimpleOrderProcessor struct {
	ctx          kprocessor.RecordProcessorContext[string, OrderEvent]
	failureCount int // Simulate occasional failures
}

func (p *SimpleOrderProcessor) Init(ctx kprocessor.RecordProcessorContext[string, OrderEvent]) error {
	p.ctx = ctx
	return nil
}

func (p *SimpleOrderProcessor) ProcessRecord(ctx context.Context, record kprocessor.Record[string, OrderEvent]) error {
	// Simulate occasional failures for demo purposes
	p.failureCount++
	if p.failureCount == 3 {
		p.failureCount = 0
		return errors.New("simulated processing error")
	}

	// Core business logic - no logging, no metrics, no tracing
	// Just pure business logic
	order := record.Value
	if order.Amount > 1000 {
		order.Status = "pending_review"
	} else {
		order.Status = "approved"
	}

	// Forward the processed order
	enrichedRecord := record
	enrichedRecord.Value = order
	p.ctx.ForwardRecord(ctx, enrichedRecord)

	return nil
}

func (p *SimpleOrderProcessor) Close() error {
	return nil
}

// ExampleBasicInterceptors demonstrates the built-in interceptors
func ExampleBasicInterceptors() {
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("Example 1: Basic Interceptors (Logging + Metrics + Retry)")
	fmt.Println(strings.Repeat("=", 80))

	// Setup logging
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Setup metrics
	var recordsProcessed atomic.Int64
	var processingTime atomic.Int64

	// Create a simple processor (just business logic)
	baseProcessor := &SimpleOrderProcessor{}

	// Wrap it with interceptors - no changes to the processor itself!
	processorWithInterceptors := kprocessor.WithInterceptors(
		baseProcessor,
		kprocessor.LoggingInterceptor[string, OrderEvent](logger),
		kprocessor.MetricsInterceptor[string, OrderEvent](&recordsProcessed, &processingTime),
		kprocessor.ErrorHandlingInterceptor[string, OrderEvent](2, 100*time.Millisecond),
	)

	// Initialize
	mockCtx := &MockContext[string, OrderEvent]{}
	processorWithInterceptors.Init(mockCtx)

	// Process some records
	fmt.Println("\n➡️  Processing 3 orders...")
	for i := 1; i <= 3; i++ {
		record := createSampleOrder(i)
		ctx := context.Background()
		err := processorWithInterceptors.ProcessRecord(ctx, record)
		if err != nil {
			fmt.Printf("❌ Error processing order %d: %v\n", i, err)
		}
	}

	// Show metrics
	fmt.Println("\n📊 Metrics Summary:")
	fmt.Printf("  Records Processed: %d\n", recordsProcessed.Load())
	fmt.Printf("  Total Time:        %v\n", time.Duration(processingTime.Load()))
	fmt.Printf("  Avg Time/Record:   %v\n", time.Duration(processingTime.Load()/int64(recordsProcessed.Load())))

	processorWithInterceptors.Close()
	fmt.Println(strings.Repeat("=", 80))
}

// ExampleCustomInterceptor demonstrates creating custom interceptors
func ExampleCustomInterceptor() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("Example 2: Custom Interceptors (Authentication + Rate Limiting + Audit)")
	fmt.Println(strings.Repeat("=", 80))

	// Custom interceptor 1: Authentication check
	authInterceptor := func(ctx context.Context, record kprocessor.Record[string, OrderEvent], handler kprocessor.ProcessorHandler[string, OrderEvent]) error {
		// Check if user-id header exists
		if userID, ok := record.Metadata.Headers.Get("user-id"); ok {
			fmt.Printf("🔐 Auth: Verified user %s\n", string(userID))
			return handler(ctx, record)
		}
		return errors.New("authentication failed: missing user-id header")
	}

	// Custom interceptor 2: Rate limiting
	var requestCount atomic.Int64
	rateLimitInterceptor := func(ctx context.Context, record kprocessor.Record[string, OrderEvent], handler kprocessor.ProcessorHandler[string, OrderEvent]) error {
		count := requestCount.Add(1)
		const rateLimit = 5

		if count > rateLimit {
			return fmt.Errorf("rate limit exceeded: %d/%d", count, rateLimit)
		}

		fmt.Printf("✓ Rate Limit: %d/%d requests\n", count, rateLimit)
		return handler(ctx, record)
	}

	// Custom interceptor 3: Audit trail
	auditInterceptor := func(ctx context.Context, record kprocessor.Record[string, OrderEvent], handler kprocessor.ProcessorHandler[string, OrderEvent]) error {
		// Add audit headers
		record.Metadata.Headers.Set("processed-at", []byte(time.Now().Format(time.RFC3339)))
		record.Metadata.Headers.Set("processor-version", []byte("v1.2.3"))
		record.Metadata.Headers.Set("environment", []byte("production"))

		fmt.Printf("📝 Audit: Added processing metadata\n")
		return handler(ctx, record)
	}

	// Create processor with custom interceptors
	processor := kprocessor.WithInterceptors(
		&SimpleOrderProcessor{},
		authInterceptor,
		rateLimitInterceptor,
		auditInterceptor,
	)

	// Initialize
	mockCtx := &MockContext[string, OrderEvent]{}
	processor.Init(mockCtx)

	// Process records
	fmt.Println("\n➡️  Processing orders with custom interceptors...")

	// This one should succeed
	record1 := createSampleOrder(1)
	record1.Metadata.Headers.Set("user-id", []byte("user-12345"))
	if err := processor.ProcessRecord(context.Background(), record1); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
	} else {
		fmt.Printf("✅ Order processed successfully\n")
	}

	// This one will fail auth
	record2 := createSampleOrder(2)
	// Intentionally no user-id header
	fmt.Println()
	if err := processor.ProcessRecord(context.Background(), record2); err != nil {
		fmt.Printf("❌ Expected error: %v\n", err)
	}

	processor.Close()
	fmt.Println(strings.Repeat("=", 80))
}

// ExampleInterceptorChaining demonstrates interceptor execution order
func ExampleInterceptorChaining() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("Example 3: Interceptor Chaining & Execution Order")
	fmt.Println(strings.Repeat("=", 80))

	// Create interceptors that show execution order
	interceptor1 := func(ctx context.Context, record kprocessor.Record[string, OrderEvent], handler kprocessor.ProcessorHandler[string, OrderEvent]) error {
		fmt.Println("  → Interceptor 1: BEFORE processing")
		err := handler(ctx, record)
		fmt.Println("  ← Interceptor 1: AFTER processing")
		return err
	}

	interceptor2 := func(ctx context.Context, record kprocessor.Record[string, OrderEvent], handler kprocessor.ProcessorHandler[string, OrderEvent]) error {
		fmt.Println("    → Interceptor 2: BEFORE processing")
		err := handler(ctx, record)
		fmt.Println("    ← Interceptor 2: AFTER processing")
		return err
	}

	interceptor3 := func(ctx context.Context, record kprocessor.Record[string, OrderEvent], handler kprocessor.ProcessorHandler[string, OrderEvent]) error {
		fmt.Println("      → Interceptor 3: BEFORE processing")
		err := handler(ctx, record)
		fmt.Println("      ← Interceptor 3: AFTER processing")
		return err
	}

	// Chain them: outer-to-inner execution
	processor := kprocessor.WithInterceptors(
		&SimpleOrderProcessor{},
		interceptor1, // Executes first (outermost)
		interceptor2, // Executes second
		interceptor3, // Executes third (innermost before actual processor)
	)

	// Initialize and process
	mockCtx := &MockContext[string, OrderEvent]{}
	processor.Init(mockCtx)

	fmt.Println("\n➡️  Processing with chained interceptors:")
	record := createSampleOrder(1)
	processor.ProcessRecord(context.Background(), record)

	fmt.Println("\n💡 Notice: Interceptors execute outer-to-inner on the way in,")
	fmt.Println("           and inner-to-outer on the way out (like middleware)")

	processor.Close()
	fmt.Println(strings.Repeat("=", 80))
}

// Helper functions
func createSampleOrder(id int) kprocessor.Record[string, OrderEvent] {
	return kprocessor.Record[string, OrderEvent]{
		Key: fmt.Sprintf("order-%d", id),
		Value: OrderEvent{
			OrderID: fmt.Sprintf("order-%d", id),
			UserID:  fmt.Sprintf("user-%d", id),
			Amount:  float64(id * 500),
			Status:  "pending",
		},
		Metadata: kprocessor.RecordMetadata{
			Topic:     "orders",
			Partition: 0,
			Offset:    int64(id),
			Timestamp: time.Now(),
			Headers:   kprocessor.NewHeaders(),
		},
	}
}

// MockContext for demonstration
type MockContext[Kout, Vout any] struct{}

func (m *MockContext[Kout, Vout]) Forward(ctx context.Context, k Kout, v Vout) {}
func (m *MockContext[Kout, Vout]) ForwardTo(ctx context.Context, k Kout, v Vout, childName string) {}
func (m *MockContext[Kout, Vout]) GetStore(name string) kprocessor.Store {
	return nil
}
func (m *MockContext[Kout, Vout]) ForwardRecord(ctx context.Context, record kprocessor.Record[Kout, Vout]) {}
func (m *MockContext[Kout, Vout]) ForwardRecordTo(ctx context.Context, record kprocessor.Record[Kout, Vout], childName string) {}
func (m *MockContext[Kout, Vout]) StreamTime() time.Time {
	return time.Now()
}
func (m *MockContext[Kout, Vout]) WallClockTime() time.Time {
	return time.Now()
}
func (m *MockContext[Kout, Vout]) RecordMetadata() kprocessor.RecordMetadata {
	return kprocessor.RecordMetadata{}
}
func (m *MockContext[Kout, Vout]) TaskID() string {
	return "task-0"
}
func (m *MockContext[Kout, Vout]) Partition() int32 {
	return 0
}
func (m *MockContext[Kout, Vout]) Schedule(interval time.Duration, pType kprocessor.PunctuationType, callback kprocessor.Punctuator) kprocessor.Cancellable {
	return nil
}
func (m *MockContext[Kout, Vout]) Headers() *kprocessor.Headers {
	return kprocessor.NewHeaders()
}

func main() {
	ExampleBasicInterceptors()
	ExampleCustomInterceptor()
	ExampleInterceptorChaining()

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("Key Takeaways:")
	fmt.Println("✓ Built-in interceptors for logging, metrics, tracing, error handling")
	fmt.Println("✓ Easy to create custom interceptors for auth, rate limiting, audit, etc.")
	fmt.Println("✓ Interceptors chain like gRPC middleware (outer-to-inner)")
	fmt.Println("✓ Clean separation: business logic vs cross-cutting concerns")
	fmt.Println("✓ No changes needed to core processor code!")
	fmt.Println(strings.Repeat("=", 80))
}
