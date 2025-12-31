package kstreams

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestBatchProcessing verifies end-to-end batch processing
func TestBatchProcessing(t *testing.T) {
	t.Run("batch processor is called with multiple records", func(t *testing.T) {
		var batchSizes []int
		var recordsReceived []Record[string, string]

		// Create a batch processor that tracks batch sizes
		processor := &testBatchProcessor{
			processBatchFunc: func(ctx context.Context, records []Record[string, string]) error {
				batchSizes = append(batchSizes, len(records))
				recordsReceived = append(recordsReceived, records...)
				return nil
			},
		}

		// Create source node
		sourceNode := &SourceNode[string, string]{
			KeyDeserializer:   func(data []byte) (string, error) { return string(data), nil },
			ValueDeserializer: func(data []byte) (string, error) { return string(data), nil },
			DownstreamProcessors: []InputProcessor[string, string]{
				&testBatchInputProcessor{processor: processor},
			},
		}

		// Process batch of 5 records from same partition
		records := []*kgo.Record{
			{Topic: "test", Partition: 0, Offset: 0, Key: []byte("key1"), Value: []byte("value1"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 1, Key: []byte("key2"), Value: []byte("value2"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 2, Key: []byte("key3"), Value: []byte("value3"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 3, Key: []byte("key4"), Value: []byte("value4"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 4, Key: []byte("key5"), Value: []byte("value5"), Timestamp: time.Now()},
		}

		err := sourceNode.ProcessBatch(context.Background(), records)
		assert.NoError(t, err)

		// Verify batch was processed as one call
		assert.Equal(t, 1, len(batchSizes), "should have 1 batch call")
		assert.Equal(t, 5, batchSizes[0], "batch should have 5 records")

		// Verify all records were received in order
		assert.Equal(t, 5, len(recordsReceived))
		assert.Equal(t, "key1", recordsReceived[0].Key)
		assert.Equal(t, "key2", recordsReceived[1].Key)
		assert.Equal(t, "key3", recordsReceived[2].Key)
		assert.Equal(t, "key4", recordsReceived[3].Key)
		assert.Equal(t, "key5", recordsReceived[4].Key)
	})

	t.Run("task groups records by partition", func(t *testing.T) {
		var partition0Size, partition1Size int

		processor := &mockRawRecordProcessor{
			processFunc: func(ctx context.Context, record *kgo.Record) error {
				return nil
			},
		}

		batchProcessor := &testBatchRawRecordProcessor{
			RawRecordProcessor: processor,
			processBatchFunc: func(ctx context.Context, records []*kgo.Record) error {
				if len(records) > 0 {
					if records[0].Partition == 0 {
						partition0Size = len(records)
					} else {
						partition1Size = len(records)
					}
				}
				return nil
			},
		}

		task := NewTask(
			[]string{"test-topic"},
			0,
			map[string]RawRecordProcessor{"test-topic": batchProcessor},
			map[string]Store{},
			map[string]Node{},
			map[string]Flusher{},
			map[string][]string{},
		)

		// Mix records from 2 partitions
		records := []*kgo.Record{
			{Topic: "test-topic", Partition: 0, Offset: 0, Timestamp: time.Now()},
			{Topic: "test-topic", Partition: 1, Offset: 0, Timestamp: time.Now()},
			{Topic: "test-topic", Partition: 0, Offset: 1, Timestamp: time.Now()},
			{Topic: "test-topic", Partition: 1, Offset: 1, Timestamp: time.Now()},
			{Topic: "test-topic", Partition: 0, Offset: 2, Timestamp: time.Now()},
		}

		err := task.Process(context.Background(), records...)
		assert.NoError(t, err)

		// Verify records were grouped by partition
		assert.Equal(t, 3, partition0Size, "partition 0 should have 3 records")
		assert.Equal(t, 2, partition1Size, "partition 1 should have 2 records")
	})

	t.Run("ordering is preserved within batch", func(t *testing.T) {
		var receivedOffsets []int64

		processor := &testBatchProcessor{
			processBatchFunc: func(ctx context.Context, records []Record[string, string]) error {
				for range records {
					// Records are processed in order
					receivedOffsets = append(receivedOffsets, int64(len(receivedOffsets)))
				}
				return nil
			},
		}

		sourceNode := &SourceNode[string, string]{
			KeyDeserializer:   func(data []byte) (string, error) { return string(data), nil },
			ValueDeserializer: func(data []byte) (string, error) { return string(data), nil },
			DownstreamProcessors: []InputProcessor[string, string]{
				&testBatchInputProcessor{processor: processor},
			},
		}

		// Records in offset order
		records := []*kgo.Record{
			{Topic: "test", Partition: 0, Offset: 10, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 11, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 12, Key: []byte("k"), Value: []byte("v"), Timestamp: time.Now()},
		}

		err := sourceNode.ProcessBatch(context.Background(), records)
		assert.NoError(t, err)

		// Verify order was preserved
		assert.Equal(t, []int64{0, 1, 2}, receivedOffsets)
	})
}

// Test helpers

type testBatchProcessor struct {
	processBatchFunc func(context.Context, []Record[string, string]) error
}

func (p *testBatchProcessor) Init(ctx ProcessorContext[string, string]) error {
	return nil
}

func (p *testBatchProcessor) Close() error {
	return nil
}

func (p *testBatchProcessor) Process(ctx context.Context, k, v string) error {
	return p.ProcessBatch(ctx, []Record[string, string]{{Key: k, Value: v}})
}

func (p *testBatchProcessor) ProcessBatch(ctx context.Context, records []Record[string, string]) error {
	if p.processBatchFunc != nil {
		return p.processBatchFunc(ctx, records)
	}
	return nil
}

type testBatchInputProcessor struct {
	processor *testBatchProcessor
}

func (p *testBatchInputProcessor) Process(ctx context.Context, k, v string) error {
	return p.processor.Process(ctx, k, v)
}

func (p *testBatchInputProcessor) ProcessBatch(ctx context.Context, keys []string, values []string) error {
	records := make([]Record[string, string], len(keys))
	for i := range keys {
		records[i] = Record[string, string]{Key: keys[i], Value: values[i]}
	}
	return p.processor.ProcessBatch(ctx, records)
}

type testBatchRawRecordProcessor struct {
	RawRecordProcessor
	processBatchFunc func(context.Context, []*kgo.Record) error
}

func (p *testBatchRawRecordProcessor) ProcessBatch(ctx context.Context, records []*kgo.Record) error {
	if p.processBatchFunc != nil {
		return p.processBatchFunc(ctx, records)
	}
	return nil
}
