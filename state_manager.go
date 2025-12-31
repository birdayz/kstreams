package kstreams

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CommitCallback is called when a checkpoint is written
// Allows stores to perform custom actions on checkpoint (e.g., compaction)
// Matches Kafka Streams' org.apache.kafka.streams.processor.StateStore.CommitCallback
type CommitCallback interface {
	OnCommit() error
}

// StateStoreMetadata tracks metadata for a single state store
// Matches Kafka Streams' ProcessorStateManager.StateStoreMetadata
//
// Three-phase offset lifecycle:
//  1. Initialization: offset = nil (unknown)
//  2. After checkpoint load: offset = checkpointOffset (or nil if no checkpoint)
//  3. During restoration: offset = lastRestoredOffset
//  4. During processing: offset updated via changelog writes
type StateStoreMetadata struct {
	// Store instance
	Store StateStore

	// Changelog partition (nil if logging disabled)
	ChangelogPartition *TopicPartition

	// Current offset (nil = unknown, will restore from beginning)
	// Points to LAST CONSUMED offset (next fetch = offset + 1)
	Offset *int64

	// Restore callback for applying changelog records
	RestoreCallback StateRestoreCallback

	// Commit callback for custom checkpoint behavior (optional)
	// Called before writing checkpoint file
	// Matches Kafka Streams' StateStoreMetadata.commitCallback
	CommitCallback CommitCallback

	// Corrupted flag (if true, state must be wiped and rebuilt)
	Corrupted bool
}

// StateManager coordinates state store lifecycle and changelog management
// Matches Kafka Streams' org.apache.kafka.streams.processor.internals.ProcessorStateManager
//
// Responsibilities:
//   - Register stores with changelog configuration
//   - Initialize offsets from checkpoint file
//   - Coordinate restoration from changelog topics
//   - Track offsets during processing
//   - Checkpoint offsets to disk
type StateManager struct {
	taskID         string
	partition      int32
	stores         map[string]*StateStoreMetadata
	client         *kgo.Client
	checkpointFile *CheckpointFile
	stateDir       string
	appID          string
	log            *slog.Logger
	restoreListener StateRestoreListener // Progress callbacks during restoration
	restorationTimeout time.Duration    // Timeout for state restoration (default: 30 minutes)
	directoryLock  *DirectoryLock       // Exclusive lock on state directory

	// Checkpoint delta optimization
	// Tracks last checkpointed offsets to avoid excessive checkpoint I/O
	// Matches Kafka Streams' StateManagerUtil.checkpointNeeded()
	lastCheckpointedOffsets map[TopicPartition]int64
}

// NewStateManager creates a new state manager for a task
//
// Parameters:
//   - taskID: Unique task identifier (e.g., "0_0" for partition 0)
//   - partition: Task partition number
//   - appID: Application ID (used for changelog topic naming)
//   - stateDir: Directory for state files and checkpoints
//   - client: Kafka client for changelog I/O
//   - log: Logger instance
//   - restoreListener: Progress callbacks during restoration (nil = no-op listener)
//   - restorationTimeout: Timeout for state restoration (0 = use default 30 minutes)
func NewStateManager(
	taskID string,
	partition int32,
	appID string,
	stateDir string,
	client *kgo.Client,
	log *slog.Logger,
	restoreListener StateRestoreListener,
	restorationTimeout time.Duration,
) *StateManager {
	checkpointPath := filepath.Join(stateDir, fmt.Sprintf("%s.checkpoint", taskID))

	// Default to no-op listener if none provided
	if restoreListener == nil {
		restoreListener = &NoOpRestoreListener{}
	}

	// Default to 30 minutes if not specified
	// Matches Kafka Streams' state.restoration.timeout.ms default (5 minutes in Kafka Streams, but we use 30 for safety)
	if restorationTimeout == 0 {
		restorationTimeout = 30 * time.Minute
	}

	// Create directory lock to prevent concurrent access
	// Matches Kafka Streams' StateDirectory lock file behavior
	directoryLock := NewDirectoryLock(stateDir)

	return &StateManager{
		taskID:                  taskID,
		partition:               partition,
		appID:                   appID,
		stores:                  make(map[string]*StateStoreMetadata),
		client:                  client,
		checkpointFile:          NewCheckpointFile(checkpointPath),
		stateDir:                stateDir,
		log:                     log.With("component", "state_manager", "task", taskID),
		restoreListener:         restoreListener,
		restorationTimeout:      restorationTimeout,
		directoryLock:           directoryLock,
		lastCheckpointedOffsets: make(map[TopicPartition]int64),
	}
}

// RegisterStore registers a state store with the manager
// Called during task initialization for each store in the topology
//
// Parameters:
//   - store: Store instance
//   - changelogEnabled: Whether changelog is enabled for this store
//   - restoreCallback: Callback for restoring from changelog (nil if logging disabled)
//   - commitCallback: Callback for custom checkpoint behavior (nil if not needed)
//
// Changelog topic naming: <appID>-<storeName>-changelog
// Changelog partition: Same as task partition
func (sm *StateManager) RegisterStore(
	store StateStore,
	changelogEnabled bool,
	restoreCallback StateRestoreCallback,
	commitCallback CommitCallback,
) error {
	storeName := store.Name()

	var changelogPartition *TopicPartition
	if changelogEnabled {
		changelogTopic := sm.changelogTopicName(storeName)
		changelogPartition = &TopicPartition{
			Topic:     changelogTopic,
			Partition: sm.partition,
		}
		sm.log.Info("Registered store with changelog",
			"store", storeName,
			"changelog_topic", changelogTopic,
			"partition", sm.partition)
	} else {
		sm.log.Info("Registered store without changelog",
			"store", storeName)
	}

	metadata := &StateStoreMetadata{
		Store:              store,
		ChangelogPartition: changelogPartition,
		Offset:             nil, // Unknown until checkpoint loaded
		RestoreCallback:    restoreCallback,
		CommitCallback:     commitCallback,
		Corrupted:          false,
	}

	sm.stores[storeName] = metadata
	return nil
}

// AcquireLock acquires an exclusive lock on the state directory
// CRITICAL: Must be called before any state operations to prevent concurrent access
// Matches Kafka Streams' StateDirectory.lock()
//
// Returns error if:
//   - Lock is already held by another process
//   - Directory doesn't exist and can't be created
//   - System doesn't support file locking
func (sm *StateManager) AcquireLock() error {
	if err := sm.directoryLock.Lock(); err != nil {
		return fmt.Errorf("failed to acquire directory lock: %w", err)
	}
	sm.log.Info("Acquired directory lock", "state_dir", sm.stateDir)
	return nil
}

// InitializeOffsetsFromCheckpoint loads checkpoint offsets from disk
// Matches Kafka Streams' ProcessorStateManager.initializeStoreOffsetsFromCheckpoint()
//
// Offset semantics:
//   - Checkpoint stores LAST CONSUMED offset
//   - Restoration starts from (checkpoint + 1)
//   - If no checkpoint: start from offset 0 (beginning)
//   - If EOS enabled + state exists + no checkpoint: FAIL (corruption detected)
//
// Returns error if checkpoint file exists but can't be read
func (sm *StateManager) InitializeOffsetsFromCheckpoint(eosEnabled bool) error {
	checkpoints, err := sm.checkpointFile.Read()
	if err != nil {
		return fmt.Errorf("read checkpoint: %w", err)
	}

	// EOS Corruption Detection: If EOS is enabled, state directory has files,
	// but no checkpoint exists, the state is corrupted
	// Matches Kafka Streams' TaskCorruptedException behavior
	if eosEnabled && len(checkpoints) == 0 {
		stateFilesExist := sm.stateDirectoryNonEmpty()
		if stateFilesExist {
			return &TaskCorruptedException{
				TaskID: sm.taskID,
				Reason: "EOS enabled but checkpoint missing with existing state - state is corrupted",
			}
		}
	}

	if len(checkpoints) == 0 {
		sm.log.Info("No checkpoint file found, will restore all stores from beginning")
	}

	for storeName, metadata := range sm.stores {
		if metadata.ChangelogPartition == nil {
			// Not logged, skip
			continue
		}

		if offset, ok := checkpoints[*metadata.ChangelogPartition]; ok {
			if offset == OFFSET_UNKNOWN {
				// Sentinel value: offset unknown
				metadata.Offset = nil
				sm.log.Info("Checkpoint has unknown offset, will restore from beginning",
					"store", storeName,
					"partition", metadata.ChangelogPartition)
			} else {
				metadata.Offset = &offset
				sm.log.Info("Loaded checkpoint offset",
					"store", storeName,
					"partition", metadata.ChangelogPartition,
					"offset", offset)
			}
		} else {
			// No checkpoint for this store
			metadata.Offset = nil
			sm.log.Info("No checkpoint for store, will restore from beginning",
				"store", storeName,
				"partition", metadata.ChangelogPartition)
		}
	}

	return nil
}

// Restore restores a batch of records to a store
// Matches Kafka Streams' ProcessorStateManager.restore()
//
// Called by changelog reader with batches of records polled from changelog topic
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

// Flush flushes all state stores to disk
// CRITICAL: Must be called BEFORE Checkpoint() to ensure data is persisted
// Matches Kafka Streams' ProcessorStateManager.flush() behavior
//
// This is separate from Checkpoint() to allow independent flushing
// without writing the checkpoint file (e.g., before EOS transaction commit)
func (sm *StateManager) Flush(ctx context.Context) error {
	// CRITICAL: Collect first error but continue flushing all stores
	// Then throw error AFTER attempting all flushes
	// This matches Kafka Streams' ProcessorStateManager.flush() behavior
	var firstFlushErr error
	for storeName, metadata := range sm.stores {
		if metadata.Corrupted {
			// Skip corrupted stores
			continue
		}

		if err := metadata.Store.Flush(ctx); err != nil {
			sm.log.Error("Failed to flush store",
				"store", storeName,
				"error", err)
			if firstFlushErr == nil {
				// Capture first error to throw later
				firstFlushErr = fmt.Errorf("failed to flush store %s: %w", storeName, err)
			}
			// Continue flushing other stores
		}
	}

	return firstFlushErr
}

// CheckpointNeeded checks if a checkpoint should be written based on offset delta
// Matches Kafka Streams' StateManagerUtil.checkpointNeeded()
//
// Returns true if:
//   - No checkpoint has been written yet (lastCheckpointedOffsets is empty)
//   - enforceCheckpoint is true (e.g., on close)
//   - Total offset delta across all stores > 10,000
//
// This optimization reduces disk I/O by skipping checkpoints when changes are minimal
func (sm *StateManager) CheckpointNeeded(enforceCheckpoint bool) bool {
	if len(sm.lastCheckpointedOffsets) == 0 {
		return false // No baseline yet
	}
	if enforceCheckpoint {
		return true
	}

	// Calculate total offset delta
	var totalDelta int64
	for _, metadata := range sm.stores {
		if metadata.ChangelogPartition == nil || metadata.Corrupted {
			continue
		}
		if !metadata.Store.Persistent() {
			continue
		}

		currentOffset := int64(0)
		if metadata.Offset != nil {
			currentOffset = *metadata.Offset
		}

		lastCheckpointed := sm.lastCheckpointedOffsets[*metadata.ChangelogPartition]
		totalDelta += currentOffset - lastCheckpointed
	}

	// Checkpoint if delta exceeds threshold (10,000 records)
	// Matches Kafka Streams' OFFSET_DELTA_THRESHOLD_FOR_CHECKPOINT
	const OFFSET_DELTA_THRESHOLD = int64(10_000)
	return totalDelta > OFFSET_DELTA_THRESHOLD
}

// Checkpoint writes current offsets to disk
// Matches Kafka Streams' ProcessorStateManager.checkpoint()
//
// CRITICAL: Caller must call Flush() BEFORE Checkpoint()
// This method only writes the checkpoint file and does NOT flush stores
//
// Called periodically during processing (e.g., after every commit interval)
//
// Only checkpoints stores that are:
//   - Persistent (in-memory stores are skipped)
//   - Logged (have changelog partition)
//   - Not corrupted
//
// Writes atomically to prevent corruption
func (sm *StateManager) Checkpoint(ctx context.Context) error {
	// Phase 1: Call commit callbacks before checkpoint
	// Allows stores to perform custom actions (e.g., RocksDB compaction)
	// Matches Kafka Streams' ProcessorStateManager.checkpoint() line 620-629
	for storeName, metadata := range sm.stores {
		if metadata.CommitCallback != nil && !metadata.Corrupted {
			if err := metadata.CommitCallback.OnCommit(); err != nil {
				return fmt.Errorf("commit callback for store %s: %w", storeName, err)
			}
		}
	}

	// Phase 2: Collect offsets for checkpoint
	checkpoints := make(map[TopicPartition]int64)

	for storeName, metadata := range sm.stores {
		if metadata.ChangelogPartition == nil {
			// Not logged, skip
			continue
		}

		// CRITICAL: Only checkpoint persistent stores
		// In-memory stores should NOT be checkpointed
		// On restart, in-memory stores must restore from beginning
		// Matches Kafka Streams' ProcessorStateManager.checkpoint() behavior
		if !metadata.Store.Persistent() {
			sm.log.Debug("Skipping checkpoint for in-memory store", "store", storeName)
			continue
		}

		if metadata.Corrupted {
			// Don't checkpoint corrupted stores
			sm.log.Warn("Skipping checkpoint for corrupted store", "store", storeName)
			continue
		}

		if metadata.Offset != nil {
			checkpoints[*metadata.ChangelogPartition] = *metadata.Offset
		} else {
			// Unknown offset: use sentinel value
			checkpoints[*metadata.ChangelogPartition] = OFFSET_UNKNOWN
		}
	}

	if len(checkpoints) == 0 {
		sm.log.Debug("No checkpoints to write (no logged stores)")
		return nil
	}

	// Phase 3: Write checkpoint atomically
	if err := sm.checkpointFile.Write(checkpoints); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}

	// Phase 4: Update lastCheckpointedOffsets for delta optimization
	// This tracks what we just wrote, so next checkpoint can calculate delta
	// Matches Kafka Streams' behavior in maybeCheckpoint()
	sm.lastCheckpointedOffsets = make(map[TopicPartition]int64)
	for tp, offset := range checkpoints {
		sm.lastCheckpointedOffsets[tp] = offset
	}

	sm.log.Debug("Checkpoint written", "stores", len(checkpoints))
	return nil
}

// ChangelogPartitionFor returns the changelog partition for a store
// Used by ProcessorContext.LogChange() to route changelog writes
//
// Returns nil if store not found or changelog disabled for store
func (sm *StateManager) ChangelogPartitionFor(storeName string) *TopicPartition {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return nil
	}
	return metadata.ChangelogPartition
}

// GetOffset returns the current offset for a store's changelog
// Returns nil if offset is unknown or store not found
func (sm *StateManager) GetOffset(storeName string) *int64 {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return nil
	}
	return metadata.Offset
}

// SetOffset updates the offset for a store's changelog
// Used during restoration and after changelog writes
func (sm *StateManager) SetOffset(storeName string, offset int64) {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return
	}
	metadata.Offset = &offset
}

// UpdateChangelogOffsets updates offsets from written changelog records
// Matches Kafka Streams' ProcessorStateManager.updateChangelogOffsets()
//
// Called by RecordCollector after successful changelog writes
// Updates offsets in batch for better performance than individual SetOffset() calls
func (sm *StateManager) UpdateChangelogOffsets(writtenOffsets map[TopicPartition]int64) {
	for tp, offset := range writtenOffsets {
		// Find store by changelog partition
		for storeName, metadata := range sm.stores {
			if metadata.ChangelogPartition != nil && *metadata.ChangelogPartition == tp {
				metadata.Offset = &offset
				sm.log.Debug("Updated changelog offset",
					"store", storeName,
					"partition", tp,
					"offset", offset)
				break
			}
		}
	}
}

// ChangelogOffsets returns current offsets for all changelog partitions
// Matches Kafka Streams' ProcessorStateManager.changelogOffsets()
//
// Returns map of TopicPartition → next offset to fetch
// Used by StoreChangelogReader to determine fetch positions
//
// Offset semantics:
//   - If offset is known: returns offset + 1 (next offset to fetch)
//   - If offset is unknown: returns 0 (fetch from beginning)
func (sm *StateManager) ChangelogOffsets() map[TopicPartition]int64 {
	offsets := make(map[TopicPartition]int64)

	for _, metadata := range sm.stores {
		if metadata.ChangelogPartition == nil {
			continue // Not logged
		}

		if metadata.Offset == nil {
			// Unknown offset: fetch from beginning
			offsets[*metadata.ChangelogPartition] = 0
		} else {
			// Return next offset to fetch (current + 1)
			offsets[*metadata.ChangelogPartition] = *metadata.Offset + 1
		}
	}

	return offsets
}

// MarkChangelogAsCorrupted marks stores as corrupted based on changelog partitions
// Matches Kafka Streams' ProcessorStateManager.markChangelogAsCorrupted()
//
// Called when changelog topic issues are detected (e.g., InvalidOffsetException)
// Corrupted stores will be skipped during checkpoint and require wipe+restore
func (sm *StateManager) MarkChangelogAsCorrupted(partitions []TopicPartition) {
	unmarked := make([]TopicPartition, 0, len(partitions))
	unmarked = append(unmarked, partitions...)

	for storeName, metadata := range sm.stores {
		if metadata.ChangelogPartition == nil {
			continue
		}

		// Check if this store's changelog is in the corrupted list
		for i, tp := range unmarked {
			if *metadata.ChangelogPartition == tp {
				metadata.Corrupted = true
				sm.log.Warn("Marked store as corrupted",
					"store", storeName,
					"partition", tp)

				// Remove from unmarked list
				unmarked = append(unmarked[:i], unmarked[i+1:]...)
				break
			}
		}
	}

	// Warn about partitions that didn't match any store
	for _, tp := range unmarked {
		sm.log.Warn("Changelog partition not found for corruption marking", "partition", tp)
	}
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
	partitionToStore := make(map[TopicPartition]string)
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
	highWatermarks := make(map[TopicPartition]int64)

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
		tp := TopicPartition{
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

			tp := TopicPartition{
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

// Close closes all stores and writes final checkpoint
func (sm *StateManager) Close(ctx context.Context) error {
	// CRITICAL: Flush stores before checkpoint
	// Matches Kafka Streams' AbstractTask.closeStateManager()
	if err := sm.Flush(ctx); err != nil {
		sm.log.Error("Failed to flush stores before close", "error", err)
		// Continue with close even if flush fails
	}

	// Checkpoint after flush
	if err := sm.Checkpoint(ctx); err != nil {
		sm.log.Error("Failed to checkpoint before close", "error", err)
		// Continue with close even if checkpoint fails
	}

	// Close all stores
	var firstErr error
	for storeName, metadata := range sm.stores {
		if err := metadata.Store.Close(); err != nil {
			sm.log.Error("Failed to close store", "store", storeName, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Release directory lock
	// CRITICAL: Must be last step (after checkpoint and store closes)
	// Matches Kafka Streams' StateDirectory.unlock()
	if sm.directoryLock != nil && sm.directoryLock.IsLocked() {
		if err := sm.directoryLock.Unlock(); err != nil {
			sm.log.Error("Failed to release directory lock", "error", err)
			if firstErr == nil {
				firstErr = err
			}
		} else {
			sm.log.Debug("Released directory lock")
		}
	}

	return firstErr
}

// DeleteCheckpoint deletes the checkpoint file
// Used in EOS mode after loading checkpoint to prevent stale data reuse
// Matches Kafka Streams' ProcessorStateManager.deleteCheckPointFileIfEOSEnabled()
func (sm *StateManager) DeleteCheckpoint() error {
	if err := sm.checkpointFile.Delete(); err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	sm.log.Debug("Deleted checkpoint file (EOS mode)")
	return nil
}

// WipeState removes all state files and checkpoints for this task
// Used when state is corrupted or task is reset
func (sm *StateManager) WipeState() error {
	// Delete checkpoint file
	if err := sm.checkpointFile.Delete(); err != nil {
		return fmt.Errorf("delete checkpoint: %w", err)
	}

	// Delete state directory
	if err := os.RemoveAll(sm.stateDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete state dir: %w", err)
	}

	sm.log.Info("Wiped state", "state_dir", sm.stateDir)
	return nil
}

// changelogTopicName generates changelog topic name
// Format: <appID>-<storeName>-changelog
// Matches Kafka Streams' ProcessorStateManager.storeChangelogTopic()
func (sm *StateManager) changelogTopicName(storeName string) string {
	return fmt.Sprintf("%s-%s-changelog", sm.appID, storeName)
}

// ListChangelogPartitions returns all changelog partitions managed by this state manager
// Used for subscribing to changelog topics during restoration
func (sm *StateManager) ListChangelogPartitions() []TopicPartition {
	var partitions []TopicPartition
	for _, metadata := range sm.stores {
		if metadata.ChangelogPartition != nil {
			partitions = append(partitions, *metadata.ChangelogPartition)
		}
	}
	return partitions
}

// GetStore returns a store by name
func (sm *StateManager) GetStore(storeName string) StateStore {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return nil
	}
	return metadata.Store
}

// stateDirectoryNonEmpty checks if the state directory contains any files
// Used for EOS corruption detection
func (sm *StateManager) stateDirectoryNonEmpty() bool {
	entries, err := os.ReadDir(sm.stateDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		sm.log.Warn("Failed to read state directory", "error", err)
		return false
	}

	// Check if there are any non-checkpoint files
	for _, entry := range entries {
		name := entry.Name()
		// Ignore checkpoint files and temp files
		if name != filepath.Base(sm.checkpointFile.path) &&
			name != filepath.Base(sm.checkpointFile.path)+".tmp" {
			return true
		}
	}

	return false
}

// TaskCorruptedException indicates that task state is corrupted
// Matches Kafka Streams' org.apache.kafka.streams.errors.TaskCorruptedException
type TaskCorruptedException struct {
	TaskID string
	Reason string
}

func (e *TaskCorruptedException) Error() string {
	return fmt.Sprintf("task %s is corrupted: %s", e.TaskID, e.Reason)
}
