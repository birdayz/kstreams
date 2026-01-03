package runtime

import (
	"context"
	"testing"
)

// TestRuntimeSinkNode_ProcessBatch_MismatchedSlices verifies that ProcessBatch
// returns an error (not panic) when keys and values have different lengths
func TestRuntimeSinkNode_ProcessBatch_MismatchedSlices(t *testing.T) {
	// Create a minimal sink node (id, client, topic, keySerializer, valueSerializer)
	sink := NewRuntimeSinkNode[string, string](
		"test-sink",
		nil, // No client needed - we're testing validation before produce
		"test-topic",
		func(k string) ([]byte, error) { return []byte(k), nil },
		func(v string) ([]byte, error) { return []byte(v), nil },
	)

	// Call with mismatched slice lengths
	keys := []string{"k1", "k2", "k3"}
	values := []string{"v1"} // Only 1 value

	// This should return an error, NOT panic
	err := sink.ProcessBatch(context.Background(), keys, values)
	if err == nil {
		t.Error("ProcessBatch should return error for mismatched slice lengths, got nil")
	}
}
