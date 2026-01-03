package statemgr

import (
	"context"
	"fmt"

	"github.com/birdayz/kstreams/internal/checkpoint"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Restore restores a batch of records to a store
// Matches Kafka Streams' ProcessorStateManager.restore()
//
// # Called by changelog reader with batches of records polled from changelog topic
//
// Parameters:
//   - storeName: Name of store to restore
//   - records: Batch of changelog records
//
// Updates metadata.Offset to point to last record in batch
func (sm *StateManager) Restore(storeName string, records []*kgo.Record) error {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return fmt.Errorf("unknown store: %s", storeName)
	}

	if metadata.RestoreCallback == nil {
		return fmt.Errorf("no restore callback for store: %s", storeName)
	}

	if len(records) == 0 {
		return nil // Empty batch, nothing to do
	}

	// Apply batch to store via callback
	if err := metadata.RestoreCallback.RestoreBatch(records); err != nil {
		metadata.Corrupted = true
		return fmt.Errorf("restore batch to store %s: %w", storeName, err)
	}

	// Update offset to last record in batch
	lastOffset := records[len(records)-1].Offset
	metadata.Offset = &lastOffset

	sm.log.Debug("Restored batch",
		"store", storeName,
		"records", len(records),
		"last_offset", lastOffset)

	return nil
}

// RestoreState restores all stores from their changelog topics
// Matches Kafka Streams' ProcessorStateManager.restore()
//
// This method:
//  1. Creates a restore consumer for changelog partitions
//  2. Fetches high watermarks (end offsets) for each partition
//  3. Seeks to the checkpoint offsets (or beginning if no checkpoint)
//  4. Polls batches of records until caught up to high watermark
//  5. Calls Restore() callback for each batch
//
// Returns error if restoration fails for any store
func (sm *StateManager) RestoreState(ctx context.Context) error {
	// Get all changelog partitions that need restoration
	changelogPartitions := sm.ListChangelogPartitions()
	if len(changelogPartitions) == 0 {
		sm.log.Info("No changelog partitions to restore")
		return nil
	}

	sm.log.Info("Starting state restoration", "partitions", len(changelogPartitions))

	// Add restoration timeout (configurable, default 30 minutes)
	// Matches Kafka Streams' state.restoration.timeout.ms
	restoreCtx, cancel := context.WithTimeout(ctx, sm.restorationTimeout)
	defer cancel()

	// Build map of changelog partition to store name and start offset
	partitionToStore := make(map[checkpoint.TopicPartition]string)
	offsets := make(map[string]map[int32]kgo.Offset)

	for storeName, metadata := range sm.stores {
		if metadata.ChangelogPartition == nil {
			continue // Not logged
		}

		partitionToStore[*metadata.ChangelogPartition] = storeName

		topic := metadata.ChangelogPartition.Topic
		partition := metadata.ChangelogPartition.Partition

		if _, ok := offsets[topic]; !ok {
			offsets[topic] = make(map[int32]kgo.Offset)
		}

		// Determine start offset for restoration
		if metadata.Offset == nil {
			// No checkpoint, restore from beginning
			offsets[topic][partition] = kgo.NewOffset().AtStart()
			sm.log.Info("Restoring from beginning", "store", storeName)
		} else {
			// Restore from checkpoint offset + 1 (we already applied offset)
			startOffset := *metadata.Offset + 1
			offsets[topic][partition] = kgo.NewOffset().At(startOffset)
			sm.log.Info("Restoring from checkpoint", "store", storeName, "offset", startOffset)
		}
	}

	// Create restore consumer (separate from main consumer)
	restoreClient, err := kgo.NewClient(
		kgo.SeedBrokers(sm.client.OptValue(kgo.SeedBrokers).([]string)...),
		kgo.ConsumePartitions(offsets),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()), // Changelog topics don't use transactions
	)
	if err != nil {
		return fmt.Errorf("create restore consumer: %w", err)
	}
	defer restoreClient.Close()

	// CRITICAL: Fetch high watermarks (end offsets) for all partitions
	// This determines when restoration is complete
	// Matches Kafka Streams' StoreChangelogReader behavior
	adminClient := kadm.NewClient(restoreClient)
	highWatermarks := make(map[checkpoint.TopicPartition]int64)

	// Build list of unique topics for kadm
	// ListEndOffsets returns offsets for ALL partitions of each topic
	topicSet := make(map[string]struct{})
	for tp := range partitionToStore {
		topicSet[tp.Topic] = struct{}{}
	}
	var topics []string
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	// Fetch end offsets (high watermarks) using kadm
	offsetsResp, err := adminClient.ListEndOffsets(restoreCtx, topics...)
	if err != nil {
		return fmt.Errorf("fetch high watermarks: %w", err)
	}

	// Process offset responses and notify listeners of restoration start
	// Note: offsetsResp includes ALL partitions of each topic, filter to only our partitions
	offsetsResp.Each(func(resp kadm.ListedOffset) {
		tp := checkpoint.TopicPartition{
			Topic:     resp.Topic,
			Partition: resp.Partition,
		}

		// Only process partitions we're responsible for
		storeName, ok := partitionToStore[tp]
		if !ok {
			return // Not our partition, skip
		}

		if resp.Err != nil {
			sm.log.Warn("Failed to fetch high watermark",
				"topic", resp.Topic,
				"partition", resp.Partition,
				"error", resp.Err)
			return
		}

		highWatermarks[tp] = resp.Offset

		// Calculate records to restore
		var startOffset int64
		if metadata := sm.stores[storeName]; metadata.Offset != nil {
			startOffset = *metadata.Offset + 1
		}
		recordsToRestore := resp.Offset - startOffset

		// CRITICAL: Always notify listener of restoration start, even if already up-to-date
		// This ensures every store gets OnRestoreStart/OnRestoreEnd lifecycle callbacks
		// Matches Kafka Streams' GlobalStateManagerImpl behavior
		sm.restoreListener.OnRestoreStart(tp, storeName, startOffset, resp.Offset)

		if recordsToRestore > 0 {
			sm.log.Info("Restoration plan",
				"store", storeName,
				"start_offset", startOffset,
				"end_offset", resp.Offset,
				"records_to_restore", recordsToRestore)
		} else {
			sm.log.Info("Store already up to date", "store", storeName)
		}
	})

	// Restoration loop: poll until caught up to high watermark
	totalRestored := 0
	storeRestoreCounts := make(map[string]int64) // Track per-store restoration counts
	var restorationErr error

	for {
		// Check timeout
		if restoreCtx.Err() != nil {
			return fmt.Errorf("restoration timeout after %v: %w", sm.restorationTimeout, restoreCtx.Err())
		}

		fetches := restoreClient.PollFetches(restoreCtx)
		if fetches.IsClientClosed() {
			return fmt.Errorf("restore client closed unexpectedly")
		}

		if err := fetches.Err(); err != nil {
			return fmt.Errorf("fetch error during restoration: %w", err)
		}

		// Process fetched records by partition
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			// Stop processing if we already encountered an error
			if restorationErr != nil {
				return
			}

			tp := checkpoint.TopicPartition{
				Topic:     partition.Topic,
				Partition: partition.Partition,
			}

			storeName, ok := partitionToStore[tp]
			if !ok {
				restorationErr = fmt.Errorf("unknown changelog partition: %s-%d", partition.Topic, partition.Partition)
				return
			}

			if len(partition.Records) == 0 {
				return
			}

			// CRITICAL: Restore batch to store - FAIL FAST on error
			// Matches Kafka Streams' behavior (throws ProcessorStateException)
			if err := sm.Restore(storeName, partition.Records); err != nil {
				restorationErr = fmt.Errorf("restore batch for store %s: %w", storeName, err)
				// Mark store as corrupted
				sm.stores[storeName].Corrupted = true
				return
			}

			batchSize := int64(len(partition.Records))
			totalRestored += int(batchSize)
			storeRestoreCounts[storeName] += batchSize

			// Get last offset in batch for listener callback
			batchEndOffset := partition.Records[len(partition.Records)-1].Offset

			sm.log.Debug("Restored batch",
				"store", storeName,
				"records", batchSize,
				"total", totalRestored)

			// Notify listener: batch restored
			sm.restoreListener.OnBatchRestored(tp, storeName, batchEndOffset, batchSize)
		})

		// Check if any errors occurred during processing
		if restorationErr != nil {
			return restorationErr
		}

		// CRITICAL: Check if we've caught up to high watermark for ALL partitions
		// Matches Kafka Streams' StoreChangelogReader.isRestoringDone()
		allCaughtUp := true
		for tp, hwm := range highWatermarks {
			if hwm == 0 {
				// Empty changelog, already caught up
				continue
			}

			storeName := partitionToStore[tp]
			metadata := sm.stores[storeName]

			// Check if we've reached the high watermark
			// Offset points to last consumed record, so we're caught up when offset == hwm - 1
			if metadata.Offset == nil || *metadata.Offset < hwm-1 {
				allCaughtUp = false
				sm.log.Debug("Store not yet caught up",
					"store", storeName,
					"current_offset", metadata.Offset,
					"high_watermark", hwm)
				break
			}
		}

		if allCaughtUp {
			sm.log.Info("Restoration complete (caught up to high watermark)", "total_records", totalRestored)

			// Notify listener: restoration complete for each store
			for tp, storeName := range partitionToStore {
				restoredCount := storeRestoreCounts[storeName]
				sm.restoreListener.OnRestoreEnd(tp, storeName, restoredCount)
			}

			break
		}
	}

	return nil
}
