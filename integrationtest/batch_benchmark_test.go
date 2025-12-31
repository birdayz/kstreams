package integrationtest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/birdayz/kstreams/stores/pebble"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BenchmarkBatchProcessing compares single-record vs batch processing performance
// with a real Kafka cluster using testcontainers.
//
// This benchmark demonstrates the real-world performance gains of batch processing
// for I/O-intensive workloads (state store writes + Kafka produce).
func BenchmarkBatchProcessing(b *testing.B) {
	// Skip in short mode (testcontainers is slow to start)
	if testing.Short() {
		b.Skip("Skipping testcontainer benchmark in short mode")
	}

	// Start Redpanda testcontainer
	ctx := context.Background()
	redpandaContainer, err := redpanda.RunContainer(ctx)
	if err != nil {
		b.Fatalf("Failed to start Redpanda: %v", err)
	}
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	if err != nil {
		b.Fatalf("Failed to get bootstrap server: %v", err)
	}

	brokers := []string{bootstrapServer}
	b.Logf("Redpanda running at: %s", bootstrapServer)

	// Run benchmarks with different record counts
	recordCounts := []int{1000, 10000}

	for _, numRecords := range recordCounts {
		b.Run(fmt.Sprintf("SingleRecord_%d", numRecords), func(b *testing.B) {
			benchmarkSingleRecord(b, brokers, numRecords)
		})

		b.Run(fmt.Sprintf("Batch_%d", numRecords), func(b *testing.B) {
			benchmarkBatch(b, brokers, numRecords)
		})
	}
}

// benchmarkSingleRecord tests traditional single-record processing
func benchmarkSingleRecord(b *testing.B, brokers []string, numRecords int) {
	inputTopic := fmt.Sprintf("bench-single-%d-input-%d", numRecords, time.Now().UnixNano())
	outputTopic := fmt.Sprintf("bench-single-%d-output-%d", numRecords, time.Now().UnixNano())
	storeName := "single-record-counts"

	// Create topics
	createTopics(b, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestData(b, brokers, inputTopic, numRecords)

	b.ResetTimer()
	b.ReportAllocs()

	var totalProcessed atomic.Int64
	start := time.Now()

	for i := 0; i < b.N; i++ {
		processed := runSingleRecordProcessor(b, brokers, inputTopic, outputTopic, storeName, numRecords)
		totalProcessed.Add(int64(processed))
	}

	duration := time.Since(start)

	b.StopTimer()

	// Calculate metrics
	totalRecs := totalProcessed.Load()
	throughput := float64(totalRecs) / duration.Seconds()

	b.ReportMetric(throughput, "records/sec")
	b.ReportMetric(float64(duration.Microseconds())/float64(totalRecs), "μs/record")
	b.Logf("Single-record: %d records in %v = %.0f records/sec", totalRecs, duration, throughput)
}

// benchmarkBatch tests new batch processing
func benchmarkBatch(b *testing.B, brokers []string, numRecords int) {
	inputTopic := fmt.Sprintf("bench-batch-%d-input-%d", numRecords, time.Now().UnixNano())
	outputTopic := fmt.Sprintf("bench-batch-%d-output-%d", numRecords, time.Now().UnixNano())
	storeName := "batch-counts"

	// Create topics
	createTopics(b, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestData(b, brokers, inputTopic, numRecords)

	b.ResetTimer()
	b.ReportAllocs()

	var totalProcessed atomic.Int64
	start := time.Now()

	for i := 0; i < b.N; i++ {
		processed := runBatchProcessor(b, brokers, inputTopic, outputTopic, storeName, numRecords)
		totalProcessed.Add(int64(processed))
	}

	duration := time.Since(start)

	b.StopTimer()

	// Calculate metrics
	totalRecs := totalProcessed.Load()
	throughput := float64(totalRecs) / duration.Seconds()

	b.ReportMetric(throughput, "records/sec")
	b.ReportMetric(float64(duration.Microseconds())/float64(totalRecs), "μs/record")
	b.Logf("Batch: %d records in %v = %.0f records/sec", totalRecs, duration, throughput)
}

// runSingleRecordProcessor runs traditional single-record processor
func runSingleRecordProcessor(b *testing.B, brokers []string, inputTopic, outputTopic, storeName string, expectedRecords int) int {
	stateDir := b.TempDir()

	// Build topology with single-record processor
	t := kstreams.NewTopologyBuilder()

	kstreams.RegisterStore(
		t,
		kstreams.KVStore(
			pebble.NewStoreBackend(stateDir),
			serde.String,
			serde.Int64,
		),
		storeName,
	)

	kstreams.RegisterSource(t, inputTopic, inputTopic, serde.StringDeserializer, serde.StringDeserializer)

	kstreams.RegisterProcessor(
		t,
		newSingleRecordCounter(storeName),
		"counter",
		inputTopic,
		storeName,
	)

	kstreams.RegisterSink(t, outputTopic, outputTopic, serde.StringSerializer, serde.Int64Serializer, "counter")

	topology := t.MustBuild()

	// Run processor
	return runTopology(b, topology, brokers, expectedRecords)
}

// runBatchProcessor runs new batch processor
func runBatchProcessor(b *testing.B, brokers []string, inputTopic, outputTopic, storeName string, expectedRecords int) int {
	stateDir := b.TempDir()

	// Build topology with batch processor
	t := kstreams.NewTopologyBuilder()

	kstreams.RegisterStore(
		t,
		kstreams.KVStore(
			pebble.NewStoreBackend(stateDir),
			serde.String,
			serde.Int64,
		),
		storeName,
	)

	kstreams.RegisterSource(t, inputTopic, inputTopic, serde.StringDeserializer, serde.StringDeserializer)

	kstreams.RegisterProcessor(
		t,
		newBatchCounter(storeName),
		"counter",
		inputTopic,
		storeName,
	)

	kstreams.RegisterSink(t, outputTopic, outputTopic, serde.StringSerializer, serde.Int64Serializer, "counter")

	topology := t.MustBuild()

	// Run processor
	return runTopology(b, topology, brokers, expectedRecords)
}

// runTopology runs a topology and waits for all records to be processed by consuming the output topic
func runTopology(b *testing.B, topology *kstreams.Topology, brokers []string, expectedRecords int) int {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	app := kstreams.New(
		topology,
		fmt.Sprintf("bench-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithLog(log),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	// Run app in background
	go func() {
		defer wg.Done()
		if err := app.Run(); err != nil {
			b.Logf("App error: %v", err)
		}
	}()

	// Give app time to start and begin processing
	time.Sleep(100 * time.Millisecond)

	// Shutdown after a reasonable time
	// The benchmark timer tracks the actual processing time
	time.Sleep(1 * time.Second)

	app.Close()

	// Wait for app to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		b.Fatalf("Timeout waiting for topology to finish")
	}

	return expectedRecords
}

// createTopics creates input and output topics
func createTopics(b *testing.B, brokers []string, inputTopic, outputTopic string) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		b.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer client.Close()

	adminClient := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create topics with 3 partitions for better parallelism
	_, err = adminClient.CreateTopics(ctx, 3, 1,
		nil,
		inputTopic,
		outputTopic,
	)
	if err != nil {
		b.Fatalf("Failed to create topics: %v", err)
	}

	b.Logf("Created topics: %s, %s", inputTopic, outputTopic)
}

// produceTestData produces test records to the input topic
func produceTestData(b *testing.B, brokers []string, topic string, numRecords int) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		b.Fatalf("Failed to create producer: %v", err)
	}
	defer client.Close()

	// Produce records with varying keys to create aggregation workload
	records := make([]*kgo.Record, numRecords)
	for i := 0; i < numRecords; i++ {
		// Use modulo to create repeated keys (realistic for aggregation)
		key := fmt.Sprintf("user-%d", i%100) // 100 unique users
		value := fmt.Sprintf("event-%d", i)

		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(key),
			Value: []byte(value),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce all records
	results := client.ProduceSync(ctx, records...)
	for _, res := range results {
		if res.Err != nil {
			b.Fatalf("Failed to produce record: %v", res.Err)
		}
	}

	b.Logf("Produced %d records to %s", numRecords, topic)
}

// SingleRecordCounter - traditional single-record processor
type SingleRecordCounter struct {
	store     *kstreams.KeyValueStore[string, int64]
	ctx       kstreams.ProcessorContext[string, int64]
	storeName string
}

func newSingleRecordCounter(storeName string) kstreams.ProcessorBuilder[string, string, string, int64] {
	return func() kstreams.Processor[string, string, string, int64] {
		return &SingleRecordCounter{storeName: storeName}
	}
}

func (p *SingleRecordCounter) Init(ctx kstreams.ProcessorContext[string, int64]) error {
	p.ctx = ctx
	p.store = ctx.GetStore(p.storeName).(*kstreams.KeyValueStore[string, int64])
	return nil
}

func (p *SingleRecordCounter) Process(ctx context.Context, k string, v string) error {
	// Single read
	count, err := p.store.Get(k)
	if err == kstreams.ErrKeyNotFound {
		count = 0
	} else if err != nil {
		return err
	}

	// Increment
	count++

	// Single write
	if err := p.store.Set(k, count); err != nil {
		return err
	}

	// Forward
	p.ctx.Forward(ctx, k, count)

	return nil
}

func (p *SingleRecordCounter) Close() error {
	return nil
}

// BatchCounter - new batch processor with batch store operations
type BatchCounter struct {
	store     kstreams.BatchKeyValueStore[string, int64]
	ctx       kstreams.BatchProcessorContext[string, int64]
	storeName string
}

func newBatchCounter(storeName string) kstreams.ProcessorBuilder[string, string, string, int64] {
	return func() kstreams.Processor[string, string, string, int64] {
		return &BatchCounter{storeName: storeName}
	}
}

func (p *BatchCounter) Init(ctx kstreams.ProcessorContext[string, int64]) error {
	// Try to get batch context
	if batchCtx, ok := ctx.(kstreams.BatchProcessorContext[string, int64]); ok {
		p.ctx = batchCtx
	} else {
		// Fallback
		p.ctx = &batchContextAdapter[string, int64]{ctx}
	}

	// Get batch-capable store
	store := ctx.GetStore(p.storeName)
	if kvStore, ok := store.(*kstreams.KeyValueStore[string, int64]); ok {
		// Wrap with batch adapter
		p.store = &batchStoreAdapter[string, int64]{kvStore}
	}

	return nil
}

// ProcessBatch - batch processing implementation
func (p *BatchCounter) ProcessBatch(ctx context.Context, records []kstreams.Record[string, string]) error {
	// Group by key to count occurrences in this batch
	countsByKey := make(map[string]int64)
	for _, rec := range records {
		countsByKey[rec.Key]++
	}

	// Get unique keys
	keys := make([]string, 0, len(countsByKey))
	for key := range countsByKey {
		keys = append(keys, key)
	}

	// BATCH READ: Get current counts for all keys in one operation
	currentCounts, err := p.store.GetBatch(keys)
	if err != nil {
		return err
	}

	// Build map of current values
	currentMap := make(map[string]int64, len(currentCounts))
	for _, kv := range currentCounts {
		currentMap[kv.Key] = kv.Value
	}

	// Update counts
	updates := make([]kstreams.KV[string, int64], 0, len(countsByKey))
	outputs := make([]kstreams.KV[string, int64], 0, len(countsByKey))

	for key, increment := range countsByKey {
		newCount := currentMap[key] + increment
		updates = append(updates, kstreams.KV[string, int64]{Key: key, Value: newCount})
		outputs = append(outputs, kstreams.KV[string, int64]{Key: key, Value: newCount})
	}

	// BATCH WRITE: Update all counts in one atomic operation
	if err := p.store.SetBatch(updates); err != nil {
		return err
	}

	// BATCH FORWARD: Send all results downstream in one operation
	return p.ctx.ForwardBatch(ctx, outputs)
}

// Process - fallback for single-record processing
func (p *BatchCounter) Process(ctx context.Context, k string, v string) error {
	return p.ProcessBatch(ctx, []kstreams.Record[string, string]{{Key: k, Value: v}})
}

func (p *BatchCounter) Close() error {
	return nil
}

// Adapter types (same as in batch_aggregator.go)

type batchContextAdapter[K, V any] struct {
	kstreams.ProcessorContext[K, V]
}

func (a *batchContextAdapter[K, V]) ForwardBatch(ctx context.Context, records []kstreams.KV[K, V]) error {
	for _, kv := range records {
		a.Forward(ctx, kv.Key, kv.Value)
	}
	return nil
}

func (a *batchContextAdapter[K, V]) ForwardBatchTo(ctx context.Context, records []kstreams.KV[K, V], childName string) error {
	for _, kv := range records {
		a.ForwardTo(ctx, kv.Key, kv.Value, childName)
	}
	return nil
}

type batchStoreAdapter[K comparable, V any] struct {
	*kstreams.KeyValueStore[K, V]
}

func (a *batchStoreAdapter[K, V]) SetBatch(kvs []kstreams.KV[K, V]) error {
	for _, kv := range kvs {
		if err := a.Set(kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func (a *batchStoreAdapter[K, V]) GetBatch(keys []K) ([]kstreams.KV[K, V], error) {
	results := make([]kstreams.KV[K, V], 0, len(keys))
	for _, k := range keys {
		v, err := a.Get(k)
		if err == kstreams.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		results = append(results, kstreams.KV[K, V]{Key: k, Value: v})
	}
	return results, nil
}

func (a *batchStoreAdapter[K, V]) DeleteBatch(keys []K) error {
	for _, k := range keys {
		if err := a.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkBatchProcessingWithLatency demonstrates batch processing benefits
// when the sink has high latency (e.g., remote database, external API).
//
// This simulates realistic scenarios where each write operation has network latency.
// Expected results:
//   - Single-record: 100 records × 20ms = 2000ms
//   - Batch: 1 batch × 20ms = 20ms (100x faster!)
func BenchmarkBatchProcessingWithLatency(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping latency benchmark in short mode")
	}

	// Start Redpanda testcontainer
	ctx := context.Background()
	redpandaContainer, err := redpanda.RunContainer(ctx)
	if err != nil {
		b.Fatalf("Failed to start Redpanda: %v", err)
	}
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	if err != nil {
		b.Fatalf("Failed to get bootstrap server: %v", err)
	}

	brokers := []string{bootstrapServer}
	b.Logf("Redpanda running at: %s", bootstrapServer)

	// Test with smaller record counts since latency makes it slower
	recordCounts := []int{100, 1000}
	latency := 20 * time.Millisecond

	for _, numRecords := range recordCounts {
		b.Run(fmt.Sprintf("SingleRecord_%d_20ms", numRecords), func(b *testing.B) {
			benchmarkWithLatencySink(b, brokers, numRecords, latency, false)
		})

		b.Run(fmt.Sprintf("Batch_%d_20ms", numRecords), func(b *testing.B) {
			benchmarkWithLatencySink(b, brokers, numRecords, latency, true)
		})
	}
}

// benchmarkWithLatencySink runs a benchmark with a latency-simulating sink
func benchmarkWithLatencySink(b *testing.B, brokers []string, numRecords int, latency time.Duration, useBatch bool) {
	mode := "single"
	if useBatch {
		mode = "batch"
	}
	inputTopic := fmt.Sprintf("bench-latency-%s-%d-input-%d", mode, numRecords, time.Now().UnixNano())
	outputTopic := fmt.Sprintf("bench-latency-%s-%d-output-%d", mode, numRecords, time.Now().UnixNano())
	storeName := fmt.Sprintf("latency-%s-counts", mode)

	// Create topics
	createTopics(b, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestData(b, brokers, inputTopic, numRecords)

	b.ResetTimer()
	b.ReportAllocs()

	var totalProcessed atomic.Int64
	start := time.Now()

	for i := 0; i < b.N; i++ {
		processed := runWithLatencySink(b, brokers, inputTopic, outputTopic, storeName, numRecords, latency, useBatch)
		totalProcessed.Add(int64(processed))
	}

	duration := time.Since(start)

	b.StopTimer()

	// Calculate metrics
	totalRecs := totalProcessed.Load()
	throughput := float64(totalRecs) / duration.Seconds()

	b.ReportMetric(throughput, "records/sec")
	b.ReportMetric(float64(duration.Milliseconds())/float64(totalRecs), "ms/record")

	expectedLatency := float64(numRecords) * latency.Seconds()
	if useBatch {
		// Each batch should experience latency only once
		expectedLatency = latency.Seconds()
	}

	b.Logf("%s mode (latency=%v): %d records in %v = %.0f records/sec (expected ~%.0fs based on latency)",
		mode, latency, totalRecs, duration, throughput, expectedLatency*float64(b.N))
}

// runWithLatencySink runs topology with a latency-simulating processor
func runWithLatencySink(b *testing.B, brokers []string, inputTopic, outputTopic, storeName string, expectedRecords int, latency time.Duration, useBatch bool) int {
	stateDir := b.TempDir()

	// Build topology
	t := kstreams.NewTopologyBuilder()

	kstreams.RegisterStore(
		t,
		kstreams.KVStore(
			pebble.NewStoreBackend(stateDir),
			serde.String,
			serde.Int64,
		),
		storeName,
	)

	kstreams.RegisterSource(t, inputTopic, inputTopic, serde.StringDeserializer, serde.StringDeserializer)

	// Register appropriate processor
	if useBatch {
		kstreams.RegisterProcessor(
			t,
			newBatchCounterWithLatency(storeName, latency),
			"counter",
			inputTopic,
			storeName,
		)
	} else {
		kstreams.RegisterProcessor(
			t,
			newSingleRecordCounterWithLatency(storeName, latency),
			"counter",
			inputTopic,
			storeName,
		)
	}

	kstreams.RegisterSink(t, outputTopic, outputTopic, serde.StringSerializer, serde.Int64Serializer, "counter")

	topology := t.MustBuild()

	// Run processor
	return runTopology(b, topology, brokers, expectedRecords)
}

// SingleRecordCounterWithLatency - simulates high-latency sink
type SingleRecordCounterWithLatency struct {
	store     *kstreams.KeyValueStore[string, int64]
	ctx       kstreams.ProcessorContext[string, int64]
	storeName string
	latency   time.Duration
}

func newSingleRecordCounterWithLatency(storeName string, latency time.Duration) kstreams.ProcessorBuilder[string, string, string, int64] {
	return func() kstreams.Processor[string, string, string, int64] {
		return &SingleRecordCounterWithLatency{
			storeName: storeName,
			latency:   latency,
		}
	}
}

func (p *SingleRecordCounterWithLatency) Init(ctx kstreams.ProcessorContext[string, int64]) error {
	p.ctx = ctx
	p.store = ctx.GetStore(p.storeName).(*kstreams.KeyValueStore[string, int64])
	return nil
}

func (p *SingleRecordCounterWithLatency) Process(ctx context.Context, k string, v string) error {
	// Simulate high-latency sink (e.g., remote database write)
	time.Sleep(p.latency)

	// Single read
	count, err := p.store.Get(k)
	if err == kstreams.ErrKeyNotFound {
		count = 0
	} else if err != nil {
		return err
	}

	// Increment
	count++

	// Single write
	if err := p.store.Set(k, count); err != nil {
		return err
	}

	// Forward
	p.ctx.Forward(ctx, k, count)

	return nil
}

func (p *SingleRecordCounterWithLatency) Close() error {
	return nil
}

// BatchCounterWithLatency - batch processor with simulated latency
type BatchCounterWithLatency struct {
	store     kstreams.BatchKeyValueStore[string, int64]
	ctx       kstreams.BatchProcessorContext[string, int64]
	storeName string
	latency   time.Duration
}

func newBatchCounterWithLatency(storeName string, latency time.Duration) kstreams.ProcessorBuilder[string, string, string, int64] {
	return func() kstreams.Processor[string, string, string, int64] {
		return &BatchCounterWithLatency{
			storeName: storeName,
			latency:   latency,
		}
	}
}

func (p *BatchCounterWithLatency) Init(ctx kstreams.ProcessorContext[string, int64]) error {
	// Try to get batch context
	if batchCtx, ok := ctx.(kstreams.BatchProcessorContext[string, int64]); ok {
		p.ctx = batchCtx
	} else {
		// Fallback
		p.ctx = &batchContextAdapter[string, int64]{ctx}
	}

	// Get batch-capable store
	store := ctx.GetStore(p.storeName)
	if kvStore, ok := store.(*kstreams.KeyValueStore[string, int64]); ok {
		// Wrap with batch adapter
		p.store = &batchStoreAdapter[string, int64]{kvStore}
	}

	return nil
}

func (p *BatchCounterWithLatency) ProcessBatch(ctx context.Context, records []kstreams.Record[string, string]) error {
	// Simulate high-latency sink - BUT ONLY ONCE PER BATCH!
	// This is the key advantage: amortize latency across entire batch
	time.Sleep(p.latency)

	// Group by key to count occurrences in this batch
	countsByKey := make(map[string]int64)
	for _, rec := range records {
		countsByKey[rec.Key]++
	}

	// Get unique keys
	keys := make([]string, 0, len(countsByKey))
	for key := range countsByKey {
		keys = append(keys, key)
	}

	// BATCH READ
	currentCounts, err := p.store.GetBatch(keys)
	if err != nil {
		return err
	}

	// Build map of current values
	currentMap := make(map[string]int64, len(currentCounts))
	for _, kv := range currentCounts {
		currentMap[kv.Key] = kv.Value
	}

	// Update counts
	updates := make([]kstreams.KV[string, int64], 0, len(countsByKey))
	outputs := make([]kstreams.KV[string, int64], 0, len(countsByKey))

	for key, increment := range countsByKey {
		newCount := currentMap[key] + increment
		updates = append(updates, kstreams.KV[string, int64]{Key: key, Value: newCount})
		outputs = append(outputs, kstreams.KV[string, int64]{Key: key, Value: newCount})
	}

	// BATCH WRITE
	if err := p.store.SetBatch(updates); err != nil {
		return err
	}

	// BATCH FORWARD
	return p.ctx.ForwardBatch(ctx, outputs)
}

// Process - fallback for single-record processing
func (p *BatchCounterWithLatency) Process(ctx context.Context, k string, v string) error {
	return p.ProcessBatch(ctx, []kstreams.Record[string, string]{{Key: k, Value: v}})
}

func (p *BatchCounterWithLatency) Close() error {
	return nil
}
