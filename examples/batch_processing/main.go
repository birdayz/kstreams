package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/processors"
	"github.com/birdayz/kstreams/serde"
	"github.com/birdayz/kstreams/stores/pebble"
	"github.com/lmittmann/tint"
)

var log = slog.New(tint.NewHandler(os.Stderr, nil))

// Example demonstrating batch processing for high-throughput scenarios.
//
// This example shows:
// 1. How to implement BatchProcessor for optimal performance
// 2. Using batch store operations (SetBatch, GetBatch)
// 3. Batch forwarding to downstream processors
//
// Performance benefits:
// - 10-50x faster for I/O-bound workloads (state stores, Kafka)
// - Reduced per-record overhead
// - Better CPU cache utilization

func main() {
	t := kstreams.NewTopologyBuilder()

	// Register state store with batch support (Pebble supports batching)
	storeName := "user-counts"
	kstreams.RegisterStore(
		t,
		kstreams.KVStore(
			pebble.NewStoreBackend("/tmp/kstreams-batch-example"),
			serde.String,
			serde.Int64,
		),
		storeName,
	)

	// Source: Read events from Kafka
	kstreams.RegisterSource(
		t,
		"user-events",
		"user-events",
		serde.StringDeserializer,
		serde.StringDeserializer,
	)

	// Processor: Count events per user using BATCH PROCESSING
	// This will process records in batches for better performance
	kstreams.RegisterProcessor(
		t,
		processors.NewBatchCountAggregator[string](storeName),
		"count-processor",
		"user-events",
		storeName, // Connect to store
	)

	// Sink: Write counts to output topic
	kstreams.RegisterSink(
		t,
		"user-counts-output",
		"user-counts-output",
		serde.StringSerializer,
		serde.Int64Serializer,
		"count-processor",
	)

	// Build and run
	topology := t.MustBuild()
	app := kstreams.New(
		topology,
		"batch-processing-example",
		kstreams.WithWorkersCount(1),
		kstreams.WithLog(log),
		kstreams.WithBrokers([]string{"localhost:9092"}),
	)

	// Graceful shutdown
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		<-c
		log.Info("Received signal. Closing app")
		app.Close()
	}()

	log.Info("Starting batch processing example")
	log.Info("Batch processing is automatically enabled when:")
	log.Info("  1. Processor implements BatchProcessor interface")
	log.Info("  2. State store supports batching (Pebble does)")
	log.Info("  3. Sink writes to Kafka (franz-go batches internally)")
	log.Info("")
	log.Info("Expected performance:")
	log.Info("  - Single-record: ~10k-50k records/sec")
	log.Info("  - Batch processing: ~100k-500k records/sec (10-50x faster!)")

	if err := app.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	log.Info("App exited")
}

// CustomBatchProcessor demonstrates how to implement your own batch processor
type CustomBatchProcessor struct {
	store kstreams.BatchKeyValueStore[string, int64]
	ctx   kstreams.BatchProcessorContext[string, string]
}

func (p *CustomBatchProcessor) Init(ctx kstreams.ProcessorContext[string, string]) error {
	// Cast to batch context
	p.ctx = ctx.(kstreams.BatchProcessorContext[string, string])

	// Get batch-capable store
	p.store = ctx.GetStore("my-store").(kstreams.BatchKeyValueStore[string, int64])

	return nil
}

// ProcessBatch demonstrates efficient batch processing
func (p *CustomBatchProcessor) ProcessBatch(ctx context.Context, records []kstreams.Record[string, string]) error {
	// ORDERING GUARANTEE:
	// - All records are from the same partition
	// - Records are in offset order: records[0].Offset < records[1].Offset < ...
	// - Safe to process sequentially or aggregate

	log.Info("Processing batch", "size", len(records))

	// Example 1: Batch aggregation
	// Group records by some key and aggregate
	aggregates := make(map[string]int64)
	for _, rec := range records {
		aggregates[rec.Key]++
	}

	// Example 2: Batch read from store
	keys := make([]string, 0, len(aggregates))
	for key := range aggregates {
		keys = append(keys, key)
	}

	currentValues, err := p.store.GetBatch(keys)
	if err != nil {
		return err
	}

	// Build map for easy lookup
	currentMap := make(map[string]int64)
	for _, kv := range currentValues {
		currentMap[kv.Key] = kv.Value
	}

	// Example 3: Batch write to store
	updates := make([]kstreams.KV[string, int64], 0, len(aggregates))
	for key, count := range aggregates {
		newValue := currentMap[key] + count
		updates = append(updates, kstreams.KV[string, int64]{
			Key:   key,
			Value: newValue,
		})
	}

	if err := p.store.SetBatch(updates); err != nil {
		return err
	}

	// Example 4: Batch forward to downstream
	outputs := make([]kstreams.KV[string, string], 0, len(records))
	for _, rec := range records {
		outputs = append(outputs, kstreams.KV[string, string]{
			Key:   rec.Key,
			Value: fmt.Sprintf("Processed: %s", rec.Value),
		})
	}

	return p.ctx.ForwardBatch(ctx, outputs)
}

// Process implements fallback for single-record processing
func (p *CustomBatchProcessor) Process(ctx context.Context, k string, v string) error {
	// Convert to batch of size 1
	return p.ProcessBatch(ctx, []kstreams.Record[string, string]{{Key: k, Value: v}})
}

func (p *CustomBatchProcessor) Close() error {
	return nil
}
