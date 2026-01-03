package execution

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/internal/runtime"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Mock implementations for testing

type mockRawRecordProcessor struct {
	processFunc func(ctx context.Context, record *kgo.Record) error
	initFunc    func() error
}

func (m *mockRawRecordProcessor) Process(ctx context.Context, record *kgo.Record) error {
	if m.processFunc != nil {
		return m.processFunc(ctx, record)
	}
	return nil
}

func (m *mockRawRecordProcessor) Init() error {
	if m.initFunc != nil {
		return m.initFunc()
	}
	return nil
}

type mockStore struct {
	initFunc  func() error
	closeFunc func() error
	flushFunc func() error
}

func (m *mockStore) Init() error {
	if m.initFunc != nil {
		return m.initFunc()
	}
	return nil
}

func (m *mockStore) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockStore) Flush() error {
	if m.flushFunc != nil {
		return m.flushFunc()
	}
	return nil
}

func (m *mockStore) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (m *mockStore) Set(key, value []byte) error {
	return nil
}

func (m *mockStore) Delete(key []byte) error {
	return nil
}

type mockNode struct {
	initFunc  func() error
	closeFunc func() error
}

func (m *mockNode) Init() error {
	if m.initFunc != nil {
		return m.initFunc()
	}
	return nil
}

func (m *mockNode) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

type mockFlusher struct {
	flushFunc func(ctx context.Context) error
}

func (m *mockFlusher) Flush(ctx context.Context) error {
	if m.flushFunc != nil {
		return m.flushFunc(ctx)
	}
	return nil
}

func TestNewTask(t *testing.T) {
	topics := []string{"topic1", "topic2"}
	partition := int32(5)
	rootNodes := map[string]runtime.RawRecordProcessor{
		"topic1": &mockRawRecordProcessor{},
	}
	stores := map[string]kprocessor.Store{
		"store1": &mockStore{},
	}
	processors := map[string]runtime.Node{
		"proc1": &mockNode{},
	}
	sinks := map[string]runtime.Flusher{
		"sink1": &mockFlusher{},
	}
	processorToStore := map[string][]string{
		"proc1": {"store1"},
	}

	task := NewTask(topics, partition, rootNodes, stores, processors, sinks, processorToStore, nil, nil, nil, "test-task")

	assert.NotZero(t, task)
	assert.Equal(t, topics, task.topics)
	assert.Equal(t, partition, task.partition)
	assert.Equal(t, rootNodes, task.rootNodes)
	assert.Equal(t, stores, task.stores)
	assert.Equal(t, processors, task.processors)
	assert.Equal(t, sinks, task.sinks)
	assert.Equal(t, processorToStore, task.processorsToStores)
	assert.NotEqual(t, nil, task.committableOffsets)
	assert.NotZero(t, task.punctuationMgr)
}

func TestTaskInit(t *testing.T) {
	t.Run("successful init", func(t *testing.T) {
		processorInitCalled := false
		storeInitCalled := false

		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					initFunc: func() error {
						storeInitCalled = true
						return nil
					},
				},
			},
			map[string]runtime.Node{
				"proc1": &mockNode{
					initFunc: func() error {
						processorInitCalled = true
						return nil
					},
				},
			},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		err := task.Init()
		assert.NoError(t, err)
		assert.True(t, processorInitCalled)
		assert.True(t, storeInitCalled)
	})

	t.Run("init with processor error", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{
				"proc1": &mockNode{
					initFunc: func() error {
						return errors.New("processor init failed")
					},
				},
			},
			map[string]runtime.Flusher{},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		err := task.Init()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "processor init failed")
	})

	t.Run("init with store error", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					initFunc: func() error {
						return errors.New("store init failed")
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		err := task.Init()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store init failed")
	})

	t.Run("init with multiple errors", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					initFunc: func() error {
						return errors.New("store error")
					},
				},
			},
			map[string]runtime.Node{
				"proc1": &mockNode{
					initFunc: func() error {
						return errors.New("processor error")
					},
				},
			},
			map[string]runtime.Flusher{},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		err := task.Init()
		assert.Error(t, err)
		// errors.Join combines multiple errors
		assert.Contains(t, err.Error(), "processor error")
		assert.Contains(t, err.Error(), "store error")
	})
}

func TestTaskProcess(t *testing.T) {
	t.Run("process single record", func(t *testing.T) {
		var processedRecord *kgo.Record
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{
					processFunc: func(ctx context.Context, record *kgo.Record) error {
						processedRecord = record
						return nil
					},
				},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		record := &kgo.Record{
			Topic:       "topic1",
			Partition:   0,
			Offset:      10,
			LeaderEpoch: 1,
			Key:         []byte("key1"),
			Value:       []byte("value1"),
			Timestamp:   time.Now(),
		}

		err := task.Process(context.Background(), record)
		assert.NoError(t, err)
		assert.Equal(t, record, processedRecord)

		// Verify offset tracking
		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 11}, offsets["topic1"])
	})

	t.Run("process multiple records", func(t *testing.T) {
		var processedCount int
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{
					processFunc: func(ctx context.Context, record *kgo.Record) error {
						processedCount++
						return nil
					},
				},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		records := []*kgo.Record{
			{Topic: "topic1", Partition: 0, Offset: 10, LeaderEpoch: 1, Timestamp: time.Now()},
			{Topic: "topic1", Partition: 0, Offset: 11, LeaderEpoch: 1, Timestamp: time.Now()},
			{Topic: "topic1", Partition: 0, Offset: 12, LeaderEpoch: 1, Timestamp: time.Now()},
		}

		err := task.Process(context.Background(), records...)
		assert.NoError(t, err)
		assert.Equal(t, 3, processedCount)

		// Verify latest offset is tracked
		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 13}, offsets["topic1"])
	})

	t.Run("process unknown topic", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		record := &kgo.Record{
			Topic:     "unknown_topic",
			Partition: 0,
			Offset:    10,
			Timestamp: time.Now(),
		}

		err := task.Process(context.Background(), record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown topic")
	})

	t.Run("process with processor error", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{
					processFunc: func(ctx context.Context, record *kgo.Record) error {
						return errors.New("processing failed")
					},
				},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		record := &kgo.Record{
			Topic:     "topic1",
			Partition: 0,
			Offset:    10,
			Timestamp: time.Now(),
		}

		err := task.Process(context.Background(), record)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to process record")
	})

	t.Run("multiple topics with different offsets", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1", "topic2"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{},
				"topic2": &mockRawRecordProcessor{},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		records := []*kgo.Record{
			{Topic: "topic1", Partition: 0, Offset: 100, LeaderEpoch: 1, Timestamp: time.Now()},
			{Topic: "topic2", Partition: 0, Offset: 50, LeaderEpoch: 2, Timestamp: time.Now()},
			{Topic: "topic1", Partition: 0, Offset: 101, LeaderEpoch: 1, Timestamp: time.Now()},
		}

		err := task.Process(context.Background(), records...)
		assert.NoError(t, err)

		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 102}, offsets["topic1"])
		assert.Equal(t, kgo.EpochOffset{Epoch: 2, Offset: 51}, offsets["topic2"])
	})
}

func TestTaskClose(t *testing.T) {
	t.Run("successful close", func(t *testing.T) {
		closeCalled := false
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					closeFunc: func() error {
						closeCalled = true
						return nil
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		err := task.Close(context.Background())
		assert.NoError(t, err)
		assert.True(t, closeCalled)
	})

	t.Run("close with error", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					closeFunc: func() error {
						return errors.New("close failed")
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		err := task.Close(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "close failed")
	})

	t.Run("close multiple stores", func(t *testing.T) {
		store1Closed := false
		store2Closed := false

		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					closeFunc: func() error {
						store1Closed = true
						return nil
					},
				},
				"store2": &mockStore{
					closeFunc: func() error {
						store2Closed = true
						return nil
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		err := task.Close(context.Background())
		assert.NoError(t, err)
		assert.True(t, store1Closed)
		assert.True(t, store2Closed)
	})
}

func TestTaskFlush(t *testing.T) {
	t.Run("flush stores and sinks", func(t *testing.T) {
		storeFlushed := false
		sinkFlushed := false

		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					flushFunc: func() error {
						storeFlushed = true
						return nil
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{
				"sink1": &mockFlusher{
					flushFunc: func(ctx context.Context) error {
						sinkFlushed = true
						return nil
					},
				},
			},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		err := task.Flush(context.Background())
		assert.NoError(t, err)
		assert.True(t, storeFlushed)
		assert.True(t, sinkFlushed)
	})

	t.Run("flush with store error", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					flushFunc: func() error {
						return errors.New("store flush failed")
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		err := task.Flush(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store flush failed")
	})

	t.Run("flush with sink error", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{
				"sink1": &mockFlusher{
					flushFunc: func(ctx context.Context) error {
						return errors.New("sink flush failed")
					},
				},
			},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		err := task.Flush(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sink flush failed")
	})

	t.Run("flush with multiple errors", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					flushFunc: func() error {
						return errors.New("store error")
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{
				"sink1": &mockFlusher{
					flushFunc: func(ctx context.Context) error {
						return errors.New("sink error")
					},
				},
			},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		err := task.Flush(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "store error")
		assert.Contains(t, err.Error(), "sink error")
	})
}

func TestTaskOffsetManagement(t *testing.T) {
	t.Run("GetOffsetsToCommit returns committable offsets", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		// Process some records
		records := []*kgo.Record{
			{Topic: "topic1", Partition: 0, Offset: 10, LeaderEpoch: 1, Timestamp: time.Now()},
			{Topic: "topic1", Partition: 0, Offset: 11, LeaderEpoch: 1, Timestamp: time.Now()},
		}
		err := task.Process(context.Background(), records...)
		assert.NoError(t, err)

		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, 1, len(offsets))
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 12}, offsets["topic1"])
	})

	t.Run("ClearOffsets clears the offsets map", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		// Process records to populate offsets
		record := &kgo.Record{
			Topic:       "topic1",
			Partition:   0,
			Offset:      10,
			LeaderEpoch: 1,
			Timestamp:   time.Now(),
		}
		err := task.Process(context.Background(), record)
		assert.NoError(t, err)

		// Verify offsets exist
		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, 1, len(offsets))

		// Clear offsets
		task.ClearOffsets()

		// Verify offsets are empty
		offsets = task.GetOffsetsToCommit()
		assert.Equal(t, 0, len(offsets))
	})

	t.Run("offsets are updated with each batch", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		// First batch
		err := task.Process(context.Background(), &kgo.Record{
			Topic: "topic1", Partition: 0, Offset: 10, LeaderEpoch: 1, Timestamp: time.Now(),
		})
		assert.NoError(t, err)
		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 11}, offsets["topic1"])

		// Second batch
		err = task.Process(context.Background(), &kgo.Record{
			Topic: "topic1", Partition: 0, Offset: 20, LeaderEpoch: 1, Timestamp: time.Now(),
		})
		assert.NoError(t, err)
		offsets = task.GetOffsetsToCommit()
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 21}, offsets["topic1"])

		// Clear and verify
		task.ClearOffsets()
		offsets = task.GetOffsetsToCommit()
		assert.Equal(t, 0, len(offsets))
	})
}

func TestTaskString(t *testing.T) {
	t.Run("single topic", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			5,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		str := task.String()
		assert.Equal(t, "[topic1]-5", str)
	})

	t.Run("multiple topics", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1", "topic2", "topic3"},
			10,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		str := task.String()
		assert.Equal(t, "[topic1 topic2 topic3]-10", str)
	})

	t.Run("partition zero", func(t *testing.T) {
		task := NewTask(
			[]string{"test"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		str := task.String()
		assert.Equal(t, "[test]-0", str)
	})
}

func TestTaskIntegration(t *testing.T) {
	t.Run("full lifecycle - init, process, flush, close", func(t *testing.T) {
		var initOrder []string
		var flushOrder []string
		var closeOrder []string

		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{},
			},
			map[string]kprocessor.Store{
				"store1": &mockStore{
					initFunc: func() error {
						initOrder = append(initOrder, "store1")
						return nil
					},
					flushFunc: func() error {
						flushOrder = append(flushOrder, "store1")
						return nil
					},
					closeFunc: func() error {
						closeOrder = append(closeOrder, "store1")
						return nil
					},
				},
			},
			map[string]runtime.Node{
				"proc1": &mockNode{
					initFunc: func() error {
						initOrder = append(initOrder, "proc1")
						return nil
					},
				},
			},
			map[string]runtime.Flusher{
				"sink1": &mockFlusher{
					flushFunc: func(ctx context.Context) error {
						flushOrder = append(flushOrder, "sink1")
						return nil
					},
				},
			},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		// 1. Init
		err := task.Init()
		assert.NoError(t, err)
		assert.Equal(t, 2, len(initOrder)) // proc1 and store1

		// 2. Process
		record := &kgo.Record{
			Topic:       "topic1",
			Partition:   0,
			Offset:      100,
			LeaderEpoch: 1,
			Timestamp:   time.Now(),
		}
		err = task.Process(context.Background(), record)
		assert.NoError(t, err)

		// 3. Flush
		err = task.Flush(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 2, len(flushOrder)) // store1 and sink1

		// 4. Verify offsets
		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 101}, offsets["topic1"])

		// 5. Clear offsets
		task.ClearOffsets()
		offsets = task.GetOffsetsToCommit()
		assert.Equal(t, 0, len(offsets))

		// 6. Close
		err = task.Close(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(closeOrder)) // store1
	})

	t.Run("error during processing doesn't affect offset tracking for successful records", func(t *testing.T) {
		processCount := 0
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{
					processFunc: func(ctx context.Context, record *kgo.Record) error {
						processCount++
						if processCount == 2 {
							return errors.New("processing error")
						}
						return nil
					},
				},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
				nil, nil, nil, "test-task",
		)

		records := []*kgo.Record{
			{Topic: "topic1", Partition: 0, Offset: 10, LeaderEpoch: 1, Timestamp: time.Now()},
			{Topic: "topic1", Partition: 0, Offset: 11, LeaderEpoch: 1, Timestamp: time.Now()},
			{Topic: "topic1", Partition: 0, Offset: 12, LeaderEpoch: 1, Timestamp: time.Now()},
		}

		err := task.Process(context.Background(), records...)
		assert.Error(t, err) // Should error on second record

		// First record was processed successfully
		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, kgo.EpochOffset{Epoch: 1, Offset: 11}, offsets["topic1"])
	})
}

func TestTaskEdgeCases(t *testing.T) {
	t.Run("empty task", func(t *testing.T) {
		task := NewTask(
			[]string{},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		// Init should succeed with no components
		err := task.Init()
		assert.NoError(t, err)

		// Flush should succeed with no components
		err = task.Flush(context.Background())
		assert.NoError(t, err)

		// Close should succeed with no components
		err = task.Close(context.Background())
		assert.NoError(t, err)
	})

	t.Run("process with no records", func(t *testing.T) {
		task := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{
				"topic1": &mockRawRecordProcessor{},
			},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		err := task.Process(context.Background())
		assert.NoError(t, err)

		offsets := task.GetOffsetsToCommit()
		assert.Equal(t, 0, len(offsets))
	})

	t.Run("task string formatting with special characters", func(t *testing.T) {
		task := NewTask(
			[]string{"topic-with-dashes", "topic.with.dots"},
			99,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{}, nil, nil, nil, "test-task",
		)

		str := task.String()
		expected := fmt.Sprintf("%v-99", []string{"topic-with-dashes", "topic.with.dots"})
		assert.Equal(t, expected, str)
	})
}
