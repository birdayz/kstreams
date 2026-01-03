package statemgr

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/birdayz/kstreams/internal/checkpoint"
	"github.com/birdayz/kstreams/kstate"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Type aliases for backward compatibility
type (
	StateStore           = kstate.StateStore
	StateRestoreCallback = kstate.StateRestoreCallback
	StateRestoreListener = kstate.StateRestoreListener
	NoOpRestoreListener  = kstate.NoOpRestoreListener
	TopicPartition       = checkpoint.TopicPartition
	CheckpointFile       = checkpoint.CheckpointFile
	DirectoryLock        = checkpoint.DirectoryLock
)

// Re-export constructors
var (
	NewCheckpointFile = checkpoint.NewCheckpointFile
	NewDirectoryLock  = checkpoint.NewDirectoryLock
)

// Re-export constants
const (
	OFFSET_UNKNOWN = checkpoint.OFFSET_UNKNOWN
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
	Store kstate.StateStore

	// Changelog partition (nil if logging disabled)
	ChangelogPartition *checkpoint.TopicPartition

	// Current offset (nil = unknown, will restore from beginning)
	// Points to LAST CONSUMED offset (next fetch = offset + 1)
	Offset *int64

	// Restore callback for applying changelog records
	RestoreCallback kstate.StateRestoreCallback

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
	taskID             string
	partition          int32
	stores             map[string]*StateStoreMetadata
	client             *kgo.Client
	checkpointFile     *checkpoint.CheckpointFile
	stateDir           string
	appID              string
	log                *slog.Logger
	restoreListener    kstate.StateRestoreListener // Progress callbacks during restoration
	restorationTimeout time.Duration               // Timeout for state restoration (default: 30 minutes)
	directoryLock      *checkpoint.DirectoryLock   // Exclusive lock on state directory

	// Checkpoint delta optimization
	// Tracks last checkpointed offsets to avoid excessive checkpoint I/O
	// Matches Kafka Streams' StateManagerUtil.checkpointNeeded()
	lastCheckpointedOffsets map[checkpoint.TopicPartition]int64
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
	restoreListener kstate.StateRestoreListener,
	restorationTimeout time.Duration,
) *StateManager {
	checkpointPath := filepath.Join(stateDir, fmt.Sprintf("%s.checkpoint", taskID))

	// Default to no-op logger if none provided
	if log == nil {
		log = slog.Default()
	}

	// Default to no-op listener if none provided
	if restoreListener == nil {
		restoreListener = &kstate.NoOpRestoreListener{}
	}

	// Default to 5 minutes if not specified
	// Matches Kafka Streams' state.restoration.timeout.ms default
	if restorationTimeout == 0 {
		restorationTimeout = 5 * time.Minute
	}

	// Create directory lock to prevent concurrent access
	// Matches Kafka Streams' StateDirectory lock file behavior
	directoryLock := checkpoint.NewDirectoryLock(stateDir)

	return &StateManager{
		taskID:                  taskID,
		partition:               partition,
		appID:                   appID,
		stores:                  make(map[string]*StateStoreMetadata),
		client:                  client,
		checkpointFile:          checkpoint.NewCheckpointFile(checkpointPath),
		stateDir:                stateDir,
		log:                     log.With("component", "state_manager", "task", taskID),
		restoreListener:         restoreListener,
		restorationTimeout:      restorationTimeout,
		directoryLock:           directoryLock,
		lastCheckpointedOffsets: make(map[checkpoint.TopicPartition]int64),
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
	store kstate.StateStore,
	changelogEnabled bool,
	restoreCallback kstate.StateRestoreCallback,
	commitCallback CommitCallback,
) error {
	storeName := store.Name()

	// Check for duplicate store registration
	if _, exists := sm.stores[storeName]; exists {
		return fmt.Errorf("store '%s' is already registered", storeName)
	}

	var changelogPartition *checkpoint.TopicPartition
	if changelogEnabled {
		changelogTopic := sm.changelogTopicName(storeName)
		changelogPartition = &checkpoint.TopicPartition{
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

// GetStore returns a store by name
func (sm *StateManager) GetStore(storeName string) kstate.StateStore {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return nil
	}
	return metadata.Store
}

// ChangelogPartitionFor returns the changelog partition for a store
// Used by ProcessorContext.LogChange() to route changelog writes
//
// Returns nil if store not found or changelog disabled for store
func (sm *StateManager) ChangelogPartitionFor(storeName string) *checkpoint.TopicPartition {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return nil
	}
	return metadata.ChangelogPartition
}

// changelogTopicName generates changelog topic name
// Format: <appID>-<storeName>-changelog
// Matches Kafka Streams' ProcessorStateManager.storeChangelogTopic()
func (sm *StateManager) changelogTopicName(storeName string) string {
	return fmt.Sprintf("%s-%s-changelog", sm.appID, storeName)
}

// ListChangelogPartitions returns all changelog partitions managed by this state manager
// Used for subscribing to changelog topics during restoration
func (sm *StateManager) ListChangelogPartitions() []checkpoint.TopicPartition {
	var partitions []checkpoint.TopicPartition
	for _, metadata := range sm.stores {
		if metadata.ChangelogPartition != nil {
			partitions = append(partitions, *metadata.ChangelogPartition)
		}
	}
	return partitions
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
		if name != filepath.Base(sm.checkpointFile.Path) &&
			name != filepath.Base(sm.checkpointFile.Path)+".tmp" {
			return true
		}
	}

	return false
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
		return true // First checkpoint - always write to establish baseline
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

// MarkChangelogAsCorrupted marks stores as corrupted based on changelog partitions
// Matches Kafka Streams' ProcessorStateManager.markChangelogAsCorrupted()
//
// Called when changelog topic issues are detected (e.g., InvalidOffsetException)
// Corrupted stores will be skipped during checkpoint and require wipe+restore
func (sm *StateManager) MarkChangelogAsCorrupted(partitions []checkpoint.TopicPartition) {
	unmarked := make([]checkpoint.TopicPartition, 0, len(partitions))
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

// TaskCorruptedException indicates that task state is corrupted
// Matches Kafka Streams' org.apache.kafka.streams.errors.TaskCorruptedException
type TaskCorruptedException struct {
	TaskID string
	Reason string
}

func (e *TaskCorruptedException) Error() string {
	return fmt.Sprintf("task %s is corrupted: %s", e.TaskID, e.Reason)
}
