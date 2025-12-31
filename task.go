package kstreams

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Task struct {
	rootNodes map[string]RawRecordProcessor // Key = topic

	stores map[string]Store

	topics    []string
	partition int32

	committableOffsets map[string]kgo.EpochOffset // Topic => offset

	processors map[string]Node

	sinks              map[string]Flusher
	processorsToStores map[string][]string

	// Punctuation manager for stream time tracking and scheduled processing
	punctuationMgr *punctuationManager

	// StateManager for changelog coordination
	stateManager *StateManager

	// Kafka client for changelog operations
	client *kgo.Client

	// Task ID for checkpoint naming
	taskID string
}

func NewTask(topics []string, partition int32, rootNodes map[string]RawRecordProcessor, stores map[string]Store, processors map[string]Node, sinks map[string]Flusher, processorToStore map[string][]string, stateManager *StateManager, client *kgo.Client, taskID string) *Task {
	return &Task{
		rootNodes:          rootNodes,
		stores:             stores,
		topics:             topics,
		partition:          partition,
		committableOffsets: map[string]kgo.EpochOffset{},
		processors:         processors,
		sinks:              sinks,
		processorsToStores: processorToStore,
		punctuationMgr:     newPunctuationManager(), // Initialize punctuation manager
		stateManager:       stateManager,
		client:             client,
		taskID:             taskID,
	}
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
			if err := t.punctuationMgr.advanceStreamTime(ctx, record.Timestamp); err != nil {
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
		if err := t.punctuationMgr.checkWallClock(ctx); err != nil {
			return fmt.Errorf("punctuation error (wall clock): %w", err)
		}
	}

	return nil
}

func (t *Task) Init() error {
	var err error

	// Initialize processors
	for _, processor := range t.processors {
		err = errors.Join(err, processor.Init())
	}

	// NOTE: Stores are already initialized during BuildTask (before restoration)
	// This matches Kafka Streams lifecycle: Build → Init → Restore → Process
	// Stores no longer need initialization here

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
	}

	// Note: Individual store closes are handled by StateManager.Close()
	// The t.stores map contains adapters, but StateManager has the actual StateStores
	// So we don't need to close them again here

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

// Flush flushes state stores and sinks.
// CRITICAL: This must be called BEFORE Checkpoint() to ensure data is persisted
// Matches Kafka Streams' StreamTask.flushCache() and maybeWriteCheckpoint()
func (t *Task) Flush(ctx context.Context) error {
	var err error

	// Flush state stores via StateManager
	if t.stateManager != nil {
		err = errors.Join(err, t.stateManager.Flush(ctx))
	}

	// Flush sinks
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
