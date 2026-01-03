package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/kprocessor"
	"go.uber.org/mock/gomock"
)

// TestInternalProcessorContext_ForwardTo_ChildNotFound verifies that ForwardTo
// records an error when the target child doesn't exist
func TestInternalProcessorContext_ForwardTo_ChildNotFound(t *testing.T) {
	pctx := NewInternalProcessorContext[string, string](
		map[string]InputProcessor[string, string]{}, // No outputs
		nil,
	)

	// Forward to non-existent child
	pctx.ForwardTo(context.Background(), "key", "value", "non-existent")

	// Should have accumulated an error
	errs := pctx.drainErrors()
	if len(errs) == 0 {
		t.Error("ForwardTo to non-existent child should accumulate error, got none")
	}
}

func TestInternalProcessorContext_Forward(t *testing.T) {
	t.Run("forwards to all outputs", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		var output1Key, output1Value string
		var output2Key, output2Value string

		output1 := NewMockStringInputProcessor[string, string](ctrl)
		output1.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, k, v string) error {
				output1Key = k
				output1Value = v
				return nil
			})

		output2 := NewMockStringInputProcessor[string, string](ctrl)
		output2.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, k, v string) error {
				output2Key = k
				output2Value = v
				return nil
			})

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		pctx.Forward(context.Background(), "test-key", "test-value")

		assert.Equal(t, "test-key", output1Key)
		assert.Equal(t, "test-value", output1Value)
		assert.Equal(t, "test-key", output2Key)
		assert.Equal(t, "test-value", output2Value)
	})

	t.Run("handles no outputs", func(t *testing.T) {
		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		// Should not panic
		pctx.Forward(context.Background(), "test-key", "test-value")
		assert.Equal(t, 0, len(pctx.outputErrors))
	})

	t.Run("collects errors from outputs", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		err1 := errors.New("output1 error")
		err2 := errors.New("output2 error")

		output1 := NewMockStringInputProcessor[string, string](ctrl)
		output1.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).Return(err1)

		output2 := NewMockStringInputProcessor[string, string](ctrl)
		output2.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).Return(err2)

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		pctx.Forward(context.Background(), "test-key", "test-value")

		assert.Equal(t, 2, len(pctx.outputErrors))
		// Map iteration order is non-deterministic, so just check both errors are present
		errStr := pctx.outputErrors[0].Error() + " " + pctx.outputErrors[1].Error()
		assert.Contains(t, errStr, "output1")
		assert.Contains(t, errStr, "output2")
	})

	t.Run("continues forwarding even if one output errors", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		output1 := NewMockStringInputProcessor[string, string](ctrl)
		output1.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("output1 error"))

		output2 := NewMockStringInputProcessor[string, string](ctrl)
		output2.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		pctx.Forward(context.Background(), "test-key", "test-value")

		assert.Equal(t, 1, len(pctx.outputErrors))
	})
}

func TestInternalProcessorContext_ForwardTo(t *testing.T) {
	t.Run("forwards to specific output", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		output1 := NewMockStringInputProcessor[string, string](ctrl)
		output1.EXPECT().Process(gomock.Any(), "test-key", "test-value").Return(nil)

		output2 := NewMockStringInputProcessor[string, string](ctrl)
		// output2 should NOT be called

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		pctx.ForwardTo(context.Background(), "test-key", "test-value", "output1")
	})

	t.Run("accumulates error if child not found", func(t *testing.T) {
		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		// Should not panic, but should accumulate error
		pctx.ForwardTo(context.Background(), "test-key", "test-value", "nonexistent")
		assert.Equal(t, 1, len(pctx.outputErrors))
		assert.Contains(t, pctx.outputErrors[0].Error(), "nonexistent")
		assert.Contains(t, pctx.outputErrors[0].Error(), "not found")
	})

	t.Run("collects error from target output", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		targetErr := errors.New("target error")

		output := NewMockStringInputProcessor[string, string](ctrl)
		output.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).Return(targetErr)

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"target": output},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		pctx.ForwardTo(context.Background(), "test-key", "test-value", "target")

		assert.Equal(t, 1, len(pctx.outputErrors))
		// Error is wrapped with context about which child failed
		assert.True(t, errors.Is(pctx.outputErrors[0], targetErr))
		assert.Contains(t, pctx.outputErrors[0].Error(), "target")
	})
}

func TestInternalProcessorContext_GetStore(t *testing.T) {
	t.Run("returns store by name", func(t *testing.T) {
		store := &mockStore{}

		pctx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores:  map[string]kprocessor.Store{"test-store": store},
		}

		retrieved := pctx.GetStore("test-store")
		assert.NotZero(t, retrieved)
	})

	t.Run("returns nil for nonexistent store", func(t *testing.T) {
		pctx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores:  map[string]kprocessor.Store{},
		}

		retrieved := pctx.GetStore("nonexistent")
		assert.Zero(t, retrieved)
	})
}

func TestInternalProcessorContext_drainErrors(t *testing.T) {
	t.Run("returns output errors", func(t *testing.T) {
		err1 := errors.New("error 1")
		err2 := errors.New("error 2")

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{err1, err2},
		}

		errs := pctx.drainErrors()
		assert.Equal(t, 2, len(errs))
		assert.Equal(t, err1, errs[0])
		assert.Equal(t, err2, errs[1])
	})

	t.Run("returns empty slice when no errors", func(t *testing.T) {
		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]kprocessor.Store{},
			outputErrors: []error{},
		}

		errs := pctx.drainErrors()
		assert.Equal(t, 0, len(errs))
	})
}

func TestInternalRecordProcessorContext_ForwardRecord(t *testing.T) {
	t.Run("forwards record to all outputs", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		var processedKey, processedValue string

		output := NewMockStringInputProcessor[string, string](ctrl)
		output.EXPECT().Process(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, k, v string) error {
				processedKey = k
				processedValue = v
				return nil
			})

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{
				outputs:      map[string]InputProcessor[string, string]{"output": output},
				stores:       map[string]kprocessor.Store{},
				outputErrors: []error{},
			},
		}

		record := kprocessor.Record[string, string]{
			Key:   "test-key",
			Value: "test-value",
		}

		pctx.ForwardRecord(context.Background(), record)

		assert.Equal(t, "test-key", processedKey)
		assert.Equal(t, "test-value", processedValue)
	})
}

func TestInternalRecordProcessorContext_ForwardRecordTo(t *testing.T) {
	t.Run("forwards record to specific output", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		output1 := NewMockStringInputProcessor[string, string](ctrl)
		output1.EXPECT().Process(gomock.Any(), "test-key", "test-value").Return(nil)

		output2 := NewMockStringInputProcessor[string, string](ctrl)
		// output2 should NOT be called

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{
				outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
				stores:       map[string]kprocessor.Store{},
				outputErrors: []error{},
			},
		}

		record := kprocessor.Record[string, string]{
			Key:   "test-key",
			Value: "test-value",
		}

		pctx.ForwardRecordTo(context.Background(), record, "output1")
	})
}

func TestInternalRecordProcessorContext_StreamTime(t *testing.T) {
	t.Run("returns stream time from punctuation manager", func(t *testing.T) {
		streamTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		punctMgr := &PunctuationManager{
			streamTime: streamTime,
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			punctuationMgr:           punctMgr,
		}

		assert.Equal(t, streamTime, pctx.StreamTime())
	})

	t.Run("returns zero time when no punctuation manager", func(t *testing.T) {
		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			punctuationMgr:           nil,
		}

		assert.Equal(t, time.Time{}, pctx.StreamTime())
	})
}

func TestInternalRecordProcessorContext_WallClockTime(t *testing.T) {
	t.Run("returns current wall clock time", func(t *testing.T) {
		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
		}

		before := time.Now()
		wallTime := pctx.WallClockTime()
		after := time.Now()

		// Wall clock time should be between before and after
		assert.True(t, !wallTime.Before(before))
		assert.True(t, !wallTime.After(after))
	})
}

func TestInternalRecordProcessorContext_RecordMetadata(t *testing.T) {
	t.Run("returns current record metadata", func(t *testing.T) {
		metadata := kprocessor.RecordMetadata{
			Topic:     "test-topic",
			Partition: 3,
			Offset:    100,
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			currentRecord:            metadata,
		}

		assert.Equal(t, metadata, pctx.RecordMetadata())
	})
}

func TestInternalRecordProcessorContext_TaskID(t *testing.T) {
	t.Run("returns task ID", func(t *testing.T) {
		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			taskID:                   "test-task-id",
		}

		assert.Equal(t, "test-task-id", pctx.TaskID())
	})
}

func TestInternalRecordProcessorContext_Partition(t *testing.T) {
	t.Run("returns partition number", func(t *testing.T) {
		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			partition:                42,
		}

		assert.Equal(t, int32(42), pctx.Partition())
	})
}

func TestInternalRecordProcessorContext_Schedule(t *testing.T) {
	t.Run("schedules punctuator", func(t *testing.T) {
		punctMgr := &PunctuationManager{
			schedules: []*PunctuationSchedule{},
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			punctuationMgr:           punctMgr,
		}

		callback := func(ctx context.Context, timestamp time.Time) error {
			return nil
		}

		cancellable := pctx.Schedule(time.Second, kprocessor.PunctuateByStreamTime, callback)

		assert.NotZero(t, cancellable)
		assert.Equal(t, 1, len(punctMgr.schedules))
	})

	t.Run("returns no-op cancellable when punctuation manager not initialized", func(t *testing.T) {
		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			punctuationMgr:           nil,
		}

		callback := func(ctx context.Context, timestamp time.Time) error { return nil }
		cancellable := pctx.Schedule(time.Second, kprocessor.PunctuateByStreamTime, callback)

		// Should return a no-op cancellable instead of panicking
		assert.NotZero(t, cancellable)

		// Calling Cancel should not panic
		cancellable.Cancel()
	})
}

func TestInternalRecordProcessorContext_Headers(t *testing.T) {
	t.Run("returns headers from current record metadata", func(t *testing.T) {
		headers := &kprocessor.Headers{}
		headers.Add("key1", []byte("value1"))

		metadata := kprocessor.RecordMetadata{
			Headers: headers,
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			currentRecord:            metadata,
		}

		retrievedHeaders := pctx.Headers()
		assert.NotZero(t, retrievedHeaders)
		assert.Equal(t, 1, retrievedHeaders.Len())
	})

	t.Run("returns nil headers when none set", func(t *testing.T) {
		metadata := kprocessor.RecordMetadata{
			Headers: nil,
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			currentRecord:            metadata,
		}

		retrievedHeaders := pctx.Headers()
		assert.Zero(t, retrievedHeaders)
	})
}
