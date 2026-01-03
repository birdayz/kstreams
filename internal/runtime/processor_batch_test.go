package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
)

// TestBatchProcessing verifies end-to-end batch processing
func TestBatchProcessing(t *testing.T) {
	t.Run("batch processor is called with multiple records", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		var batchKeys, batchValues []string

		mockProcessor := NewMockStringBatchInputProcessor[string, string](ctrl)
		mockProcessor.EXPECT().ProcessBatch(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, keys, values []string) error {
				batchKeys = keys
				batchValues = values
				return nil
			})

		// Create source node
		sourceNode := NewRuntimeSourceNode(
			"test-source",
			"test-topic",
			func(data []byte) (string, error) { return string(data), nil },
			func(data []byte) (string, error) { return string(data), nil },
		)
		sourceNode.AddDownstream(mockProcessor)

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

		// Verify batch was processed
		assert.Equal(t, 5, len(batchKeys), "batch should have 5 keys")
		assert.Equal(t, 5, len(batchValues), "batch should have 5 values")

		// Verify all records were received in order
		assert.Equal(t, "key1", batchKeys[0])
		assert.Equal(t, "key2", batchKeys[1])
		assert.Equal(t, "key3", batchKeys[2])
		assert.Equal(t, "key4", batchKeys[3])
		assert.Equal(t, "key5", batchKeys[4])
	})

	// NOTE: "task groups records by partition" test moved to internal/execution package

	t.Run("ordering is preserved within batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		var receivedKeys []string

		mockProcessor := NewMockStringBatchInputProcessor[string, string](ctrl)
		mockProcessor.EXPECT().ProcessBatch(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, keys, values []string) error {
				receivedKeys = keys
				return nil
			})

		sourceNode := NewRuntimeSourceNode(
			"test-source",
			"test-topic",
			func(data []byte) (string, error) { return string(data), nil },
			func(data []byte) (string, error) { return string(data), nil },
		)
		sourceNode.AddDownstream(mockProcessor)

		// Records in offset order
		records := []*kgo.Record{
			{Topic: "test", Partition: 0, Offset: 10, Key: []byte("k1"), Value: []byte("v"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 11, Key: []byte("k2"), Value: []byte("v"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 12, Key: []byte("k3"), Value: []byte("v"), Timestamp: time.Now()},
		}

		err := sourceNode.ProcessBatch(context.Background(), records)
		assert.NoError(t, err)

		// Verify order was preserved
		assert.Equal(t, []string{"k1", "k2", "k3"}, receivedKeys)
	})

	t.Run("falls back to single record processing when batch not supported", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		var processedKeys []string

		// Use non-batch processor (StringInputProcessor doesn't have ProcessBatch)
		mockProcessor := NewMockStringInputProcessor[string, string](ctrl)
		mockProcessor.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, k, v string) error {
				processedKeys = append(processedKeys, k)
				return nil
			}).Times(3)

		sourceNode := NewRuntimeSourceNode(
			"test-source",
			"test-topic",
			func(data []byte) (string, error) { return string(data), nil },
			func(data []byte) (string, error) { return string(data), nil },
		)
		sourceNode.AddDownstream(mockProcessor)

		records := []*kgo.Record{
			{Topic: "test", Partition: 0, Offset: 0, Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 1, Key: []byte("k2"), Value: []byte("v2"), Timestamp: time.Now()},
			{Topic: "test", Partition: 0, Offset: 2, Key: []byte("k3"), Value: []byte("v3"), Timestamp: time.Now()},
		}

		err := sourceNode.ProcessBatch(context.Background(), records)
		assert.NoError(t, err)

		// Verify all records were processed individually
		assert.Equal(t, []string{"k1", "k2", "k3"}, processedKeys)
	})
}

// TestProcessorNodeBatch_MismatchedSlices verifies that ProcessBatch handles
// mismatched keys/values slices gracefully (returns error, not panic)
func TestProcessorNodeBatch_MismatchedSlices(t *testing.T) {
	// Create a mock batch processor
	mock := &mockBatchProcessor[string, string, string, string]{}

	ctx := NewInternalProcessorContext[string, string](nil, nil)
	node := NewProcessorNodeBatch[string, string, string, string](mock, ctx)

	// Call ProcessBatch with mismatched slice lengths - this should NOT panic
	keys := []string{"k1", "k2", "k3"}
	values := []string{"v1"} // Only 1 value, but 3 keys

	// This currently panics with "index out of range" - should return error instead
	err := node.ProcessBatch(context.Background(), keys, values)
	if err == nil {
		t.Error("Expected error for mismatched slice lengths, got nil")
	}
}

// mockBatchProcessor is a simple mock for testing (implements BatchProcessor)
type mockBatchProcessor[Kin, Vin, Kout, Vout any] struct {
	ctx kprocessor.ProcessorContext[Kout, Vout]
}

func (m *mockBatchProcessor[Kin, Vin, Kout, Vout]) Init(ctx kprocessor.ProcessorContext[Kout, Vout]) error {
	m.ctx = ctx
	return nil
}

func (m *mockBatchProcessor[Kin, Vin, Kout, Vout]) Close() error {
	return nil
}

func (m *mockBatchProcessor[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	return nil
}

func (m *mockBatchProcessor[Kin, Vin, Kout, Vout]) ProcessBatch(ctx context.Context, records []kprocessor.Record[Kin, Vin]) error {
	return nil
}

// TestForwardBatchTo_ChildNotFound verifies that ForwardBatchTo returns an error
// when the target child doesn't exist (instead of silently dropping records)
func TestForwardBatchTo_ChildNotFound(t *testing.T) {
	// Create context with no outputs
	ctx := &InternalBatchProcessorContext[string, string]{
		InternalProcessorContext: NewInternalProcessorContext[string, string](nil, nil),
	}

	// Try to forward to a non-existent child
	records := []kprocessor.KV[string, string]{
		{Key: "k1", Value: "v1"},
		{Key: "k2", Value: "v2"},
	}

	err := ctx.ForwardBatchTo(context.Background(), records, "non-existent-child")

	// Should return an error, not silently succeed
	if err == nil {
		t.Error("ForwardBatchTo should return error when child not found, got nil (records silently dropped)")
	}
}
