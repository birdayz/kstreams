package kstreams

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestInternalProcessorContext_Forward(t *testing.T) {
	t.Run("forwards to all outputs", func(t *testing.T) {
		var output1Called, output2Called bool
		var output1Key, output1Value string
		var output2Key, output2Value string

		output1 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				output1Called = true
				output1Key = k
				output1Value = v
				return nil
			},
		}

		output2 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				output2Called = true
				output2Key = k
				output2Value = v
				return nil
			},
		}

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		pctx.Forward(context.Background(), "test-key", "test-value")

		assert.True(t, output1Called)
		assert.True(t, output2Called)
		assert.Equal(t, "test-key", output1Key)
		assert.Equal(t, "test-value", output1Value)
		assert.Equal(t, "test-key", output2Key)
		assert.Equal(t, "test-value", output2Value)
	})

	t.Run("handles no outputs", func(t *testing.T) {
		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		// Should not panic
		pctx.Forward(context.Background(), "test-key", "test-value")
		assert.Equal(t, 0, len(pctx.outputErrors))
	})

	t.Run("collects errors from outputs", func(t *testing.T) {
		err1 := errors.New("output1 error")
		err2 := errors.New("output2 error")

		output1 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				return err1
			},
		}

		output2 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				return err2
			},
		}

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]Store{},
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
		var output2Called bool

		output1 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				return errors.New("output1 error")
			},
		}

		output2 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				output2Called = true
				return nil
			},
		}

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		pctx.Forward(context.Background(), "test-key", "test-value")

		assert.True(t, output2Called)
		assert.Equal(t, 1, len(pctx.outputErrors))
	})
}

func TestInternalProcessorContext_ForwardTo(t *testing.T) {
	t.Run("forwards to specific output", func(t *testing.T) {
		var output1Called, output2Called bool

		output1 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				output1Called = true
				return nil
			},
		}

		output2 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				output2Called = true
				return nil
			},
		}

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		pctx.ForwardTo(context.Background(), "test-key", "test-value", "output1")

		assert.True(t, output1Called)
		assert.False(t, output2Called)
	})

	t.Run("does nothing if child not found", func(t *testing.T) {
		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		// Should not panic
		pctx.ForwardTo(context.Background(), "test-key", "test-value", "nonexistent")
		assert.Equal(t, 0, len(pctx.outputErrors))
	})

	t.Run("collects error from target output", func(t *testing.T) {
		targetErr := errors.New("target error")

		output := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				return targetErr
			},
		}

		pctx := &InternalProcessorContext[string, string]{
			outputs:      map[string]InputProcessor[string, string]{"target": output},
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		pctx.ForwardTo(context.Background(), "test-key", "test-value", "target")

		assert.Equal(t, 1, len(pctx.outputErrors))
		assert.Equal(t, targetErr, pctx.outputErrors[0])
	})
}

func TestInternalProcessorContext_GetStore(t *testing.T) {
	t.Run("returns store by name", func(t *testing.T) {
		store := &mockStore{}

		pctx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores:  map[string]Store{"test-store": store},
		}

		retrieved := pctx.GetStore("test-store")
		assert.NotZero(t, retrieved)
	})

	t.Run("returns nil for nonexistent store", func(t *testing.T) {
		pctx := &InternalProcessorContext[string, string]{
			outputs: map[string]InputProcessor[string, string]{},
			stores:  map[string]Store{},
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
			stores:       map[string]Store{},
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
			stores:       map[string]Store{},
			outputErrors: []error{},
		}

		errs := pctx.drainErrors()
		assert.Equal(t, 0, len(errs))
	})
}

func TestInternalRecordProcessorContext_ForwardRecord(t *testing.T) {
	t.Run("forwards record to all outputs", func(t *testing.T) {
		var processedKey, processedValue string

		output := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				processedKey = k
				processedValue = v
				return nil
			},
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{
				outputs:      map[string]InputProcessor[string, string]{"output": output},
				stores:       map[string]Store{},
				outputErrors: []error{},
			},
		}

		record := Record[string, string]{
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
		var output1Called, output2Called bool

		output1 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				output1Called = true
				return nil
			},
		}

		output2 := &mockInputProcessor[string, string]{
			processFunc: func(ctx context.Context, k, v string) error {
				output2Called = true
				return nil
			},
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{
				outputs:      map[string]InputProcessor[string, string]{"output1": output1, "output2": output2},
				stores:       map[string]Store{},
				outputErrors: []error{},
			},
		}

		record := Record[string, string]{
			Key:   "test-key",
			Value: "test-value",
		}

		pctx.ForwardRecordTo(context.Background(), record, "output1")

		assert.True(t, output1Called)
		assert.False(t, output2Called)
	})
}

func TestInternalRecordProcessorContext_StreamTime(t *testing.T) {
	t.Run("returns stream time from punctuation manager", func(t *testing.T) {
		streamTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		punctMgr := &punctuationManager{
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
		metadata := RecordMetadata{
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
		punctMgr := &punctuationManager{
			schedules: []*PunctuationSchedule{},
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			punctuationMgr:           punctMgr,
		}

		callback := func(ctx context.Context, timestamp time.Time) error {
			return nil
		}

		cancellable := pctx.Schedule(time.Second, PunctuateByStreamTime, callback)

		assert.NotZero(t, cancellable)
		assert.Equal(t, 1, len(punctMgr.schedules))
	})

	t.Run("panics when punctuation manager not initialized", func(t *testing.T) {
		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			punctuationMgr:           nil,
		}

		defer func() {
			r := recover()
			assert.NotZero(t, r)
			assert.Contains(t, r.(string), "punctuation manager not initialized")
		}()

		callback := func(ctx context.Context, timestamp time.Time) error { return nil }
		pctx.Schedule(time.Second, PunctuateByStreamTime, callback)
	})
}

func TestInternalRecordProcessorContext_Headers(t *testing.T) {
	t.Run("returns headers from current record metadata", func(t *testing.T) {
		headers := &Headers{}
		headers.Add("key1", []byte("value1"))

		metadata := RecordMetadata{
			Headers: headers,
		}

		pctx := &InternalRecordProcessorContext[string, string]{
			InternalProcessorContext: &InternalProcessorContext[string, string]{},
			currentRecord:            metadata,
		}

		retrievedHeaders := pctx.Headers()
		assert.NotZero(t, retrievedHeaders)
		assert.Equal(t, 1, len(retrievedHeaders.headers))
	})

	t.Run("returns nil headers when none set", func(t *testing.T) {
		metadata := RecordMetadata{
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
