package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/birdayz/kstreams/kprocessor"
)

// PaymentEvent represents a payment transaction
type PaymentEvent struct {
	UserID string
	Amount float64
	Status string
}

// EnrichedPaymentProcessor is a RecordProcessor that demonstrates:
// - Full metadata access (topic, partition, offset, timestamp)
// - Header reading and writing for tracing/correlation
// - Enhanced context with record forwarding
type EnrichedPaymentProcessor struct {
	ctx kprocessor.RecordProcessorContext[string, PaymentEvent]
}

func (p *EnrichedPaymentProcessor) Init(ctx kprocessor.RecordProcessorContext[string, PaymentEvent]) error {
	p.ctx = ctx
	fmt.Println("✓ EnrichedPaymentProcessor initialized with RecordProcessorContext")
	return nil
}

func (p *EnrichedPaymentProcessor) ProcessRecord(ctx context.Context, record kprocessor.Record[string, PaymentEvent]) error {
	// ========================================
	// FEATURE 1: Access Full Record Metadata
	// ========================================
	fmt.Printf("\n📝 Processing Record:\n")
	fmt.Printf("  Topic:      %s\n", record.Metadata.Topic)
	fmt.Printf("  Partition:  %d\n", record.Metadata.Partition)
	fmt.Printf("  Offset:     %d\n", record.Metadata.Offset)
	fmt.Printf("  Timestamp:  %s\n", record.Metadata.Timestamp.Format(time.RFC3339))
	fmt.Printf("  Key:        %s\n", record.Key)
	fmt.Printf("  Amount:     $%.2f\n", record.Value.Amount)

	// ========================================
	// FEATURE 2: Read Headers (Tracing/Correlation)
	// ========================================
	if traceID, ok := record.Metadata.Headers.Get("trace-id"); ok {
		fmt.Printf("  Trace ID:   %s\n", string(traceID))
	}

	if correlationID, ok := record.Metadata.Headers.Get("correlation-id"); ok {
		fmt.Printf("  Correlation ID: %s\n", string(correlationID))
	}

	// ========================================
	// FEATURE 3: Business Logic
	// ========================================
	payment := record.Value
	var newStatus string

	if payment.Amount > 10000 {
		newStatus = "pending_approval"
		fmt.Printf("  💰 High-value payment - requires approval\n")
	} else {
		newStatus = "approved"
		fmt.Printf("  ✓ Payment auto-approved\n")
	}

	// ========================================
	// FEATURE 4: Write Headers (Add Metadata)
	// ========================================
	record.Metadata.Headers.Set("processed-by", []byte("EnrichedPaymentProcessor"))
	record.Metadata.Headers.Set("processed-at", []byte(time.Now().Format(time.RFC3339)))
	record.Metadata.Headers.Set("processor-version", []byte("v2.0.1"))

	// ========================================
	// FEATURE 5: Forward Record with Metadata
	// ========================================
	enrichedPayment := PaymentEvent{
		UserID: payment.UserID,
		Amount: payment.Amount,
		Status: newStatus,
	}

	enrichedRecord := kprocessor.Record[string, PaymentEvent]{
		Key:      record.Key,
		Value:    enrichedPayment,
		Metadata: record.Metadata, // Preserve all metadata
	}

	p.ctx.ForwardRecord(ctx, enrichedRecord)
	fmt.Printf("  ➡️  Forwarded with enriched metadata\n")

	return nil
}

func (p *EnrichedPaymentProcessor) Close() error {
	fmt.Println("✓ EnrichedPaymentProcessor closed")
	return nil
}

// ExampleRecordProcessorWithMetadata demonstrates all RecordProcessor features
func ExampleRecordProcessorWithMetadata() {
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("RecordProcessor Example: Headers & Metadata Access")
	fmt.Println(strings.Repeat("=", 70))

	// Create a sample record with full metadata
	sampleRecord := kprocessor.Record[string, PaymentEvent]{
		Key: "user-12345",
		Value: PaymentEvent{
			UserID: "user-12345",
			Amount: 15000.00,
			Status: "pending",
		},
		Metadata: kprocessor.RecordMetadata{
			Topic:     "payments",
			Partition: 0,
			Offset:    42,
			Timestamp: time.Now(),
			Headers:   kprocessor.NewHeaders(),
		},
	}

	// Add tracing headers
	sampleRecord.Metadata.Headers.Set("trace-id", []byte("trace-abc-123"))
	sampleRecord.Metadata.Headers.Set("correlation-id", []byte("corr-xyz-789"))

	// Create and initialize processor
	processor := &EnrichedPaymentProcessor{}

	// Create a mock context (in real use, this would be provided by the framework)
	mockCtx := &MockRecordProcessorContext[string, PaymentEvent]{}
	processor.Init(mockCtx)

	// Process the record
	ctx := context.Background()
	if err := processor.ProcessRecord(ctx, sampleRecord); err != nil {
		log.Fatalf("Failed to process record: %v", err)
	}

	// Close processor
	processor.Close()

	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("Key Takeaways:")
	fmt.Println("✓ Full access to topic, partition, offset, timestamp")
	fmt.Println("✓ Read headers for tracing and correlation")
	fmt.Println("✓ Write headers to add metadata")
	fmt.Println("✓ Forward records with preserved/enriched metadata")
	fmt.Println(strings.Repeat("=", 70))
}

// MockRecordProcessorContext is a simple mock for demonstration
type MockRecordProcessorContext[Kout, Vout any] struct{}

func (m *MockRecordProcessorContext[Kout, Vout]) Forward(ctx context.Context, k Kout, v Vout) {
	fmt.Printf("  [Mock] Forwarded: %v -> %v\n", k, v)
}

func (m *MockRecordProcessorContext[Kout, Vout]) ForwardTo(ctx context.Context, k Kout, v Vout, childName string) {
	fmt.Printf("  [Mock] ForwardedTo %s: %v -> %v\n", childName, k, v)
}

func (m *MockRecordProcessorContext[Kout, Vout]) GetStore(name string) kprocessor.Store {
	return nil
}

func (m *MockRecordProcessorContext[Kout, Vout]) ForwardRecord(ctx context.Context, record kprocessor.Record[Kout, Vout]) {
	fmt.Printf("  [Mock] ForwardRecord: key=%v, headers=%d\n", record.Key, record.Metadata.Headers.Len())
}

func (m *MockRecordProcessorContext[Kout, Vout]) ForwardRecordTo(ctx context.Context, record kprocessor.Record[Kout, Vout], childName string) {
	fmt.Printf("  [Mock] ForwardRecordTo %s: key=%v\n", childName, record.Key)
}

func (m *MockRecordProcessorContext[Kout, Vout]) StreamTime() time.Time {
	return time.Now()
}

func (m *MockRecordProcessorContext[Kout, Vout]) WallClockTime() time.Time {
	return time.Now()
}

func (m *MockRecordProcessorContext[Kout, Vout]) RecordMetadata() kprocessor.RecordMetadata {
	return kprocessor.RecordMetadata{}
}

func (m *MockRecordProcessorContext[Kout, Vout]) TaskID() string {
	return "task-0"
}

func (m *MockRecordProcessorContext[Kout, Vout]) Partition() int32 {
	return 0
}

func (m *MockRecordProcessorContext[Kout, Vout]) Schedule(interval time.Duration, pType kprocessor.PunctuationType, callback kprocessor.Punctuator) kprocessor.Cancellable {
	return nil
}

func (m *MockRecordProcessorContext[Kout, Vout]) Headers() *kprocessor.Headers {
	return kprocessor.NewHeaders()
}

func main() {
	ExampleRecordProcessorWithMetadata()
}
