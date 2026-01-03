package execution

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/internal/coordination"
	"github.com/birdayz/kstreams/internal/runtime"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
)

func newTestTaskManager(pgs []*coordination.PartitionGroup) *TaskManager {
	// Create a minimal topology for testing
	tb := kdag.NewBuilder()
	topo, _ := tb.Build() // Empty topology

	return &TaskManager{
		tasks:    []*Task{},
		client:   nil, // Not needed for unit tests
		log:      slog.Default(),
		topology: topo,
		pgs:      pgs,
	}
}

func TestMatchingPGs(t *testing.T) {
	t.Run("single partition group - all topics present", func(t *testing.T) {
		pg := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1", "topic2"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg})

		assigned := map[string][]int32{
			"topic1": {0, 1},
			"topic2": {0, 1},
		}

		pgs, err := tm.matchingPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pgs))
		assert.Equal(t, pg, pgs[0])
	})

	t.Run("single partition group - no topics present", func(t *testing.T) {
		pg := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1", "topic2"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg})

		assigned := map[string][]int32{
			"topic3": {0, 1},
		}

		matchingPGs, err := tm.matchingPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(matchingPGs)) // No matching PGs
	})

	t.Run("single partition group - partial topics (error)", func(t *testing.T) {
		pg := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1", "topic2"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg})

		assigned := map[string][]int32{
			"topic1": {0, 1},
			// topic2 missing!
		}

		_, err := tm.matchingPGs(assigned)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "source topics missing")
	})

	t.Run("multiple partition groups", func(t *testing.T) {
		pg1 := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1"},
		}
		pg2 := &coordination.PartitionGroup{
			SourceTopics: []string{"topic2", "topic3"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg1, pg2})

		assigned := map[string][]int32{
			"topic1": {0},
			"topic2": {0},
			"topic3": {0},
		}

		matchingPGs, err := tm.matchingPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(matchingPGs))
	})

	t.Run("multiple partition groups - some matching", func(t *testing.T) {
		pg1 := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1"},
		}
		pg2 := &coordination.PartitionGroup{
			SourceTopics: []string{"topic2", "topic3"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg1, pg2})

		assigned := map[string][]int32{
			"topic1": {0}, // Only pg1 topics present
		}

		matchingPGs, err := tm.matchingPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(matchingPGs))
		assert.Equal(t, pg1, matchingPGs[0])
	})
}

func TestFindPGs(t *testing.T) {
	t.Run("single topic partition group", func(t *testing.T) {
		pg := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg})

		assigned := map[string][]int32{
			"topic1": {0, 1, 2},
		}

		pgPartitions, err := tm.findPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pgPartitions))
		assert.Equal(t, pg, pgPartitions[0].partitionGroup)
		assert.Equal(t, []int32{0, 1, 2}, pgPartitions[0].partitions)
	})

	t.Run("co-partitioned topics - matching partitions", func(t *testing.T) {
		pg := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1", "topic2"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg})

		assigned := map[string][]int32{
			"topic1": {0, 1, 2},
			"topic2": {0, 1, 2}, // Same partitions
		}

		pgPartitions, err := tm.findPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pgPartitions))
		assert.Equal(t, []int32{0, 1, 2}, pgPartitions[0].partitions)
	})

	t.Run("co-partitioned topics - mismatched partitions (error)", func(t *testing.T) {
		pg := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1", "topic2"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg})

		assigned := map[string][]int32{
			"topic1": {0, 1, 2},
			"topic2": {0, 1}, // Different partitions!
		}

		_, err := tm.findPGs(assigned)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "partitions not co-assigned")
	})

	t.Run("partitions are sorted", func(t *testing.T) {
		pg := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg})

		assigned := map[string][]int32{
			"topic1": {2, 0, 1}, // Unsorted
		}

		pgPartitions, err := tm.findPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, []int32{0, 1, 2}, pgPartitions[0].partitions) // Should be sorted
	})

	t.Run("multiple partition groups", func(t *testing.T) {
		pg1 := &coordination.PartitionGroup{
			SourceTopics: []string{"topic1"},
		}
		pg2 := &coordination.PartitionGroup{
			SourceTopics: []string{"topic2", "topic3"},
		}
		tm := newTestTaskManager([]*coordination.PartitionGroup{pg1, pg2})

		assigned := map[string][]int32{
			"topic1": {0, 1},
			"topic2": {5, 6},
			"topic3": {5, 6}, // Co-partitioned with topic2
		}

		pgPartitions, err := tm.findPGs(assigned)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(pgPartitions))

		// Verify pg1
		assert.Equal(t, pg1, pgPartitions[0].partitionGroup)
		assert.Equal(t, []int32{0, 1}, pgPartitions[0].partitions)

		// Verify pg2
		assert.Equal(t, pg2, pgPartitions[1].partitionGroup)
		assert.Equal(t, []int32{5, 6}, pgPartitions[1].partitions)
	})
}

func TestTaskManagerClose(t *testing.T) {
	t.Run("close all tasks", func(t *testing.T) {
		closedTasks := []int32{}

		task0 := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store": &mockStore{
					closeFunc: func() error {
						closedTasks = append(closedTasks, 0)
						return nil
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
			nil, nil, nil, "task-0",
		)

		task1 := NewTask(
			[]string{"topic1"},
			1,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store": &mockStore{
					closeFunc: func() error {
						closedTasks = append(closedTasks, 1)
						return nil
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
			nil, nil, nil, "task-1",
		)

		tm := &TaskManager{
			tasks:    []*Task{task0, task1},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		err := tm.Close(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 2, len(closedTasks))
	})

	t.Run("close with errors", func(t *testing.T) {
		task0 := NewTask(
			[]string{"topic1"},
			0,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store": &mockStore{
					closeFunc: func() error {
						return errors.New("close error 1")
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
			nil, nil, nil, "task-0",
		)

		task1 := NewTask(
			[]string{"topic1"},
			1,
			map[string]runtime.RawRecordProcessor{},
			map[string]kprocessor.Store{
				"store": &mockStore{
					closeFunc: func() error {
						return errors.New("close error 2")
					},
				},
			},
			map[string]runtime.Node{},
			map[string]runtime.Flusher{},
			map[string][]string{},
			nil, nil, nil, "task-1",
		)

		tm := &TaskManager{
			tasks:    []*Task{task0, task1},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		err := tm.Close(context.Background())
		assert.Error(t, err)
		// errors.Join combines multiple errors
		assert.Contains(t, err.Error(), "close error")
	})

	t.Run("close empty task manager", func(t *testing.T) {
		tm := &TaskManager{
			tasks:    []*Task{},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		err := tm.Close(context.Background())
		assert.NoError(t, err)
	})
}

func TestTaskFor(t *testing.T) {
	t.Run("find existing task", func(t *testing.T) {
		task0 := NewTask([]string{"topic1"}, 0, map[string]runtime.RawRecordProcessor{}, map[string]kprocessor.Store{}, map[string]runtime.Node{}, map[string]runtime.Flusher{}, map[string][]string{}, nil, nil, nil, "task-0")
		task1 := NewTask([]string{"topic1"}, 1, map[string]runtime.RawRecordProcessor{}, map[string]kprocessor.Store{}, map[string]runtime.Node{}, map[string]runtime.Flusher{}, map[string][]string{}, nil, nil, nil, "task-1")
		task2 := NewTask([]string{"topic2"}, 0, map[string]runtime.RawRecordProcessor{}, map[string]kprocessor.Store{}, map[string]runtime.Node{}, map[string]runtime.Flusher{}, map[string][]string{}, nil, nil, nil, "task-0")

		tm := &TaskManager{
			tasks:    []*Task{task0, task1, task2},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		task, err := tm.TaskFor("topic1", 1)
		assert.NoError(t, err)
		assert.Equal(t, task1, task)
	})

	t.Run("task not found - wrong topic", func(t *testing.T) {
		task0 := NewTask([]string{"topic1"}, 0, map[string]runtime.RawRecordProcessor{}, map[string]kprocessor.Store{}, map[string]runtime.Node{}, map[string]runtime.Flusher{}, map[string][]string{}, nil, nil, nil, "task-0")

		tm := &TaskManager{
			tasks:    []*Task{task0},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		task, err := tm.TaskFor("topic2", 0)
		assert.Error(t, err)
		assert.Equal(t, ErrTaskNotFound, err)
		assert.Zero(t, task)
	})

	t.Run("task not found - wrong partition", func(t *testing.T) {
		task0 := NewTask([]string{"topic1"}, 0, map[string]runtime.RawRecordProcessor{}, map[string]kprocessor.Store{}, map[string]runtime.Node{}, map[string]runtime.Flusher{}, map[string][]string{}, nil, nil, nil, "task-0")

		tm := &TaskManager{
			tasks:    []*Task{task0},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		task, err := tm.TaskFor("topic1", 99)
		assert.Error(t, err)
		assert.Equal(t, ErrTaskNotFound, err)
		assert.Zero(t, task)
	})

	t.Run("empty task manager", func(t *testing.T) {
		tm := &TaskManager{
			tasks:    []*Task{},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		task, err := tm.TaskFor("topic1", 0)
		assert.Error(t, err)
		assert.Equal(t, ErrTaskNotFound, err)
		assert.Zero(t, task)
	})

	t.Run("task with multiple topics", func(t *testing.T) {
		// Co-partitioned topics
		task0 := NewTask([]string{"topic1", "topic2"}, 0, map[string]runtime.RawRecordProcessor{}, map[string]kprocessor.Store{}, map[string]runtime.Node{}, map[string]runtime.Flusher{}, map[string][]string{}, nil, nil, nil, "task-0")

		tm := &TaskManager{
			tasks:    []*Task{task0},
			client:   nil,
			log:      slog.Default(),
			topology: nil,
			pgs:      nil,
		}

		// Should find task by either topic
		task, err := tm.TaskFor("topic1", 0)
		assert.NoError(t, err)
		assert.Equal(t, task0, task)

		task, err = tm.TaskFor("topic2", 0)
		assert.NoError(t, err)
		assert.Equal(t, task0, task)
	})
}

// TestTask_InitFailure_CleanupResources verifies that if Task.Init() fails,
// resources are properly cleaned up (Close() is called)
func TestTask_InitFailure_CleanupResources(t *testing.T) {
	// Create a mock store that fails on Init()
	failingStore := &mockFailingStore{
		name:    "failing-store",
		initErr: errors.New("init failed"),
	}
	workingStore := &mockFailingStore{
		name:      "working-store",
		closeChan: make(chan struct{}, 1),
	}

	// Create a task with both stores
	task := NewTask(
		[]string{"topic1"},
		0,
		map[string]runtime.RawRecordProcessor{},
		map[string]kprocessor.Store{
			"working-store": workingStore,
			"failing-store": failingStore,
		},
		map[string]runtime.Node{},
		map[string]runtime.Flusher{},
		map[string][]string{},
		nil,
		nil,
		nil,
		"test-task",
	)

	// Init should fail
	err := task.Init()
	if err == nil {
		t.Fatal("Expected Init to fail")
	}

	// Close the task to cleanup
	_ = task.Close(context.Background())

	// Verify working store was closed (cleanup happened)
	select {
	case <-workingStore.closeChan:
		// Good - Close was called
	default:
		t.Error("Working store was not closed after Init failure - resource leak!")
	}
}

// mockFailingStore is a store that can fail on Init
type mockFailingStore struct {
	name      string
	initErr   error
	closeChan chan struct{}
}

func (m *mockFailingStore) Name() string       { return m.name }
func (m *mockFailingStore) Persistent() bool   { return false }
func (m *mockFailingStore) Init() error        { return m.initErr }
func (m *mockFailingStore) Flush() error       { return nil }
func (m *mockFailingStore) Close() error {
	if m.closeChan != nil {
		select {
		case m.closeChan <- struct{}{}:
		default:
		}
	}
	return nil
}

