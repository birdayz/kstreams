package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/birdayz/kstreams/internal/runtime"
	"github.com/birdayz/kstreams/internal/statemgr"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TaskConfig holds configuration for creating a Task.
// Use NewTaskWithConfig for cleaner task construction.
type TaskConfig struct {
	TaskID             string
	Topics             []string
	Partition          int32
	RootNodes          map[string]runtime.RawRecordProcessor
	Stores             map[string]kprocessor.Store
	Processors         map[string]runtime.Node
	Sinks              map[string]runtime.Flusher
	ProcessorsToStores map[string][]string
	StateManager       *statemgr.StateManager
	Client             *kgo.Client
	Collector          *RecordCollector
}

type Task struct {
	rootNodes map[string]runtime.RawRecordProcessor // Key = topic

	stores map[string]kprocessor.Store

	topics    []string
	partition int32

	committableOffsets map[string]kgo.EpochOffset // Topic => offset

	processors map[string]runtime.Node

	sinks              map[string]runtime.Flusher
	processorsToStores map[string][]string

	// Punctuation manager for stream time tracking and scheduled processing
	punctuationMgr *runtime.PunctuationManager

	// StateManager for changelog coordination
	stateManager *statemgr.StateManager

	// Kafka client for changelog operations
	client *kgo.Client

	// RecordCollector for batching changelog writes
	collector *RecordCollector

	// Task ID for checkpoint naming
	taskID string
}

// NewTaskWithConfig creates a new Task from a TaskConfig.
func NewTaskWithConfig(cfg TaskConfig) *Task {
	return &Task{
		taskID:             cfg.TaskID,
		topics:             cfg.Topics,
		partition:          cfg.Partition,
		rootNodes:          cfg.RootNodes,
		stores:             cfg.Stores,
		processors:         cfg.Processors,
		sinks:              cfg.Sinks,
		processorsToStores: cfg.ProcessorsToStores,
		stateManager:       cfg.StateManager,
		client:             cfg.Client,
		collector:          cfg.Collector,
		committableOffsets: make(map[string]kgo.EpochOffset),
		punctuationMgr:     runtime.NewPunctuationManager(),
	}
}

// NewTask creates a new Task.
// Deprecated: Use NewTaskWithConfig for cleaner construction.
func NewTask(topics []string, partition int32, rootNodes map[string]runtime.RawRecordProcessor, stores map[string]kprocessor.Store, processors map[string]runtime.Node, sinks map[string]runtime.Flusher, processorToStore map[string][]string, stateManager *statemgr.StateManager, client *kgo.Client, collector *RecordCollector, taskID string) *Task {
	return NewTaskWithConfig(TaskConfig{
		TaskID:             taskID,
		Topics:             topics,
		Partition:          partition,
		RootNodes:          rootNodes,
		Stores:             stores,
		Processors:         processors,
		Sinks:              sinks,
		ProcessorsToStores: processorToStore,
		StateManager:       stateManager,
		Client:             client,
		Collector:          collector,
	})
}

func (t *Task) Process(ctx context.Context, records ...*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}

	// Group records by (topic, partition) to maintain ordering guarantees
	// CRITICAL: Each batch must be from the same partition and in offset order
	type partitionKey struct {
		topic     string
		partition int32
	}

	batches := make(map[partitionKey][]*kgo.Record)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		batches[key] = append(batches[key], record)
	}

	// Process each partition's batch independently
	for key, batch := range batches {
		p, ok := t.rootNodes[key.topic]
		if !ok {
			return fmt.Errorf("unknown topic: %s", key.topic)
		}

		// Advance stream time to the latest timestamp in batch
		// (punctuators need current stream time)
		for _, record := range batch {
			if err := t.punctuationMgr.AdvanceStreamTime(ctx, record.Timestamp); err != nil {
				return fmt.Errorf("punctuation error (stream time): %w", err)
			}
		}

		// Try batch processing first
		if batchSource, ok := p.(BatchRawRecordProcessor); ok {
			if err := batchSource.ProcessBatch(ctx, batch); err != nil {
				return fmt.Errorf("failed to process batch: %w", err)
			}
			// Update committable offset (last record in batch)
			lastRecord := batch[len(batch)-1]
			t.committableOffsets[key.topic] = kgo.EpochOffset{
				Epoch:  lastRecord.LeaderEpoch,
				Offset: lastRecord.Offset + 1,
			}
		} else {
			// Fallback: process one-by-one
			// This maintains the old behavior where partial progress is tracked
			for _, record := range batch {
				if err := p.Process(ctx, record); err != nil {
					return fmt.Errorf("failed to process record: %w", err)
				}
				// Update offset after each successful record
				t.committableOffsets[key.topic] = kgo.EpochOffset{
					Epoch:  record.LeaderEpoch,
					Offset: record.Offset + 1,
				}
			}
		}

		// Check wall clock punctuators after processing batch
		if err := t.punctuationMgr.CheckWallClock(ctx); err != nil {
			return fmt.Errorf("punctuation error (wall clock): %w", err)
		}
	}

	return nil
}

func (t *Task) Init() error {
	var err error

	// Initialize stores
	// When created via BuildTaskFromGraph, stores are pre-initialized during restoration
	// When created via NewTask (legacy/tests), stores need initialization here
	for _, store := range t.stores {
		err = errors.Join(err, store.Init())
	}

	// Initialize processors
	for _, processor := range t.processors {
		err = errors.Join(err, processor.Init())
	}

	return err
}

func (t *Task) Close(ctx context.Context) error {
	var err error

	// CRITICAL: Close StateManager first (writes final checkpoint, then closes stores)
	// This matches Kafka Streams' StreamTask.close() → StateManager.close()
	// StateManager.Close() will:
	// 1. Write final checkpoint
	// 2. Close all registered stores
	if t.stateManager != nil {
		if closeErr := t.stateManager.Close(ctx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close state manager: %w", closeErr))
		}
	} else {
		// Fallback for tasks created without StateManager (legacy/tests)
		for _, store := range t.stores {
			err = errors.Join(err, store.Close())
		}
	}

	return err
}

func (t *Task) GetOffsetsToCommit() map[string]kgo.EpochOffset {
	return t.committableOffsets
}

func (t *Task) ClearOffsets() {
	for k := range t.committableOffsets {
		delete(t.committableOffsets, k)
	}
}

// Flush flushes changelog records, state stores, and sinks.
// CRITICAL: This must be called BEFORE Checkpoint() to ensure data is persisted
// Matches Kafka Streams' StreamTask.flushCache() and maybeWriteCheckpoint()
//
// Flush order:
// 1. RecordCollector.Flush() - batch produce changelog records, get offsets
// 2. StateManager offset update - update offsets from collector results
// 3. StateManager.Flush() - flush state stores to disk
// 4. Sinks.Flush() - flush output records
func (t *Task) Flush(ctx context.Context) error {
	var err error

	// Step 1: Flush changelog collector (batch produce buffered records)
	if t.collector != nil {
		offsets, flushErr := t.collector.Flush(ctx)
		if flushErr != nil {
			err = errors.Join(err, fmt.Errorf("flush changelog collector: %w", flushErr))
		} else if t.stateManager != nil && len(offsets) > 0 {
			// Step 2: Update StateManager with produced offsets
			if updateErr := t.stateManager.UpdateChangelogOffsetsByStore(offsets); updateErr != nil {
				err = errors.Join(err, fmt.Errorf("update changelog offsets: %w", updateErr))
			}
		}
	}

	// Step 3: Flush state stores via StateManager
	if t.stateManager != nil {
		err = errors.Join(err, t.stateManager.Flush(ctx))
	} else {
		// Fallback for tasks created without StateManager (legacy/tests)
		for _, store := range t.stores {
			err = errors.Join(err, store.Flush())
		}
	}

	// Step 4: Flush sinks
	for _, sink := range t.sinks {
		err = errors.Join(err, sink.Flush(ctx))
	}

	return err
}

// Checkpoint writes the current state offsets to the checkpoint file
// CRITICAL: Must be called after every commit to ensure checkpoint reflects committed state
// Matches Kafka Streams' StreamTask.postCommit() -> stateMgr.checkpoint()
func (t *Task) Checkpoint(ctx context.Context) error {
	if t.stateManager == nil {
		return nil // No state manager (stateless task)
	}
	return t.stateManager.Checkpoint(ctx)
}

func (t *Task) String() string {
	return fmt.Sprintf("%v-%d", t.topics, t.partition)
}
