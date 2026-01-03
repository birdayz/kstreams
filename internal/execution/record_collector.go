package execution

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RecordCollector buffers changelog records and produces them in batches.
// This matches Kafka Streams' RecordCollector pattern for improved throughput.
//
// Thread-safety: RecordCollector is NOT thread-safe by design.
// Each Task has its own RecordCollector, and Tasks run in a single goroutine.
type RecordCollector struct {
	client *kgo.Client

	// Buffer for changelog records
	// Each record is tagged with store name for offset tracking after produce
	buffer []bufferedRecord

	// Pending offsets - populated after Flush() with offsets from produce results
	// Map: storeName -> lastOffset
	pendingOffsets map[string]int64

	mu sync.Mutex // Guards buffer access (for safety, though single-goroutine)
}

// bufferedRecord pairs a Kafka record with its store name
// The store name is needed to update offsets after batch produce
type bufferedRecord struct {
	record    *kgo.Record
	storeName string
}

// NewRecordCollector creates a new record collector for a task
func NewRecordCollector(client *kgo.Client) *RecordCollector {
	return &RecordCollector{
		client:         client,
		buffer:         make([]bufferedRecord, 0, 1000), // Pre-allocate for common case
		pendingOffsets: make(map[string]int64),
	}
}

// Send buffers a changelog record for later batch production.
// Returns immediately without blocking on Kafka.
//
// Parameters:
//   - storeName: Name of the state store (for offset tracking)
//   - topic: Changelog topic
//   - partition: Changelog partition
//   - key: Record key (serialized)
//   - value: Record value (serialized, nil for tombstone)
//   - timestamp: Event timestamp (from source record)
func (rc *RecordCollector) Send(storeName, topic string, partition int32, key, value []byte, timestamp time.Time) {
	record := &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	rc.mu.Lock()
	rc.buffer = append(rc.buffer, bufferedRecord{
		record:    record,
		storeName: storeName,
	})
	rc.mu.Unlock()
}

// Flush produces all buffered records to Kafka and returns offset updates.
// This should be called before committing offsets.
//
// Returns a map of storeName -> lastOffset for updating StateManager.
// The offsets represent the last successfully produced changelog offset per store.
func (rc *RecordCollector) Flush(ctx context.Context) (map[string]int64, error) {
	rc.mu.Lock()
	if len(rc.buffer) == 0 {
		rc.mu.Unlock()
		return nil, nil
	}

	// Take ownership of buffer
	toFlush := rc.buffer
	rc.buffer = make([]bufferedRecord, 0, cap(toFlush))
	rc.mu.Unlock()

	// Reset pending offsets
	rc.pendingOffsets = make(map[string]int64)

	// Produce all records in a batch
	// Use async Produce with callback to track results
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	var offsetMu sync.Mutex

	for _, br := range toFlush {
		wg.Add(1)
		storeName := br.storeName // Capture for closure
		rc.client.Produce(ctx, br.record, func(r *kgo.Record, err error) {
			defer wg.Done()

			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("changelog produce for store %s: %w", storeName, err)
				}
				errMu.Unlock()
				return
			}

			// Track the highest offset per store
			offsetMu.Lock()
			if current, exists := rc.pendingOffsets[storeName]; !exists || r.Offset > current {
				rc.pendingOffsets[storeName] = r.Offset
			}
			offsetMu.Unlock()
		})
	}

	// Wait for all produces to complete
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	// Return copy of offsets
	result := make(map[string]int64, len(rc.pendingOffsets))
	for k, v := range rc.pendingOffsets {
		result[k] = v
	}

	return result, nil
}

// BufferSize returns the current number of buffered records
func (rc *RecordCollector) BufferSize() int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return len(rc.buffer)
}
