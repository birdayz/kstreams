package statemgr

import (
	"context"
	"fmt"

	"github.com/birdayz/kstreams/internal/checkpoint"
)

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
			if offset == checkpoint.OFFSET_UNKNOWN {
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
	checkpoints := make(map[checkpoint.TopicPartition]int64)

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
			checkpoints[*metadata.ChangelogPartition] = checkpoint.OFFSET_UNKNOWN
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
	sm.lastCheckpointedOffsets = make(map[checkpoint.TopicPartition]int64)
	for tp, offset := range checkpoints {
		sm.lastCheckpointedOffsets[tp] = offset
	}

	sm.log.Debug("Checkpoint written", "stores", len(checkpoints))
	return nil
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
