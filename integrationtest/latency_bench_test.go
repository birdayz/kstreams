package integrationtest

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/birdayz/kstreams"
)

// BenchmarkLatencyAmortization demonstrates the core benefit of batch processing:
// amortizing latency across multiple records.
//
// This is a simplified benchmark that directly measures the latency amortization
// without Kafka/testcontainer overhead.
//
// Expected results with 20ms latency per operation:
//   - Single-record (100 recs): 100 × 20ms = 2000ms
//   - Batch (100 recs):         1 × 20ms   = 20ms    (100x faster!)
func BenchmarkLatencyAmortization(b *testing.B) {
	latencies := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
	}

	recordCounts := []int{50, 100, 500}

	for _, latency := range latencies {
		for _, numRecords := range recordCounts {
			b.Run(fmt.Sprintf("SingleRecord_%d_%dms", numRecords, latency.Milliseconds()), func(b *testing.B) {
				benchmarkLatencySingleRecord(b, numRecords, latency)
			})

			b.Run(fmt.Sprintf("Batch_%d_%dms", numRecords, latency.Milliseconds()), func(b *testing.B) {
				benchmarkLatencyBatch(b, numRecords, latency)
			})
		}
	}
}

func benchmarkLatencySingleRecord(b *testing.B, numRecords int, latency time.Duration) {
	// Create test records
	records := make([]kstreams.Record[string, string], numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = kstreams.Record[string, string]{
			Key:   fmt.Sprintf("key-%d", i%10), // 10 unique keys
			Value: fmt.Sprintf("value-%d", i),
		}
	}

	var opsCompleted atomic.Int64

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		// Process each record individually with latency
		for _, rec := range records {
			// Simulate high-latency operation (e.g., remote database write, API call)
			time.Sleep(latency)

			// Simulate some work
			_ = rec.Key + rec.Value

			opsCompleted.Add(1)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	totalOps := opsCompleted.Load()
	throughput := float64(totalOps) / elapsed.Seconds()

	b.ReportMetric(throughput, "ops/sec")
	b.ReportMetric(float64(elapsed.Milliseconds())/float64(totalOps), "ms/op")

	expectedTime := time.Duration(numRecords*b.N) * latency
	b.Logf("Single-record: %d ops in %v (expected ~%v) = %.0f ops/sec",
		totalOps, elapsed, expectedTime, throughput)
}

func benchmarkLatencyBatch(b *testing.B, numRecords int, latency time.Duration) {
	// Create test records
	records := make([]kstreams.Record[string, string], numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = kstreams.Record[string, string]{
			Key:   fmt.Sprintf("key-%d", i%10), // 10 unique keys
			Value: fmt.Sprintf("value-%d", i),
		}
	}

	var opsCompleted atomic.Int64

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		// Process entire batch with latency ONLY ONCE
		// This is the key advantage: latency is amortized across all records
		time.Sleep(latency)

		// Simulate batch work
		for _, rec := range records {
			_ = rec.Key + rec.Value
			opsCompleted.Add(1)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()

	totalOps := opsCompleted.Load()
	throughput := float64(totalOps) / elapsed.Seconds()

	b.ReportMetric(throughput, "ops/sec")
	b.ReportMetric(float64(elapsed.Milliseconds())/float64(totalOps), "ms/op")

	expectedTime := time.Duration(b.N) * latency
	speedup := float64(numRecords*b.N) * latency.Seconds() / elapsed.Seconds()
	b.Logf("Batch: %d ops in %v (expected ~%v) = %.0f ops/sec (%.1fx faster)",
		totalOps, elapsed, expectedTime, throughput, speedup)
}
