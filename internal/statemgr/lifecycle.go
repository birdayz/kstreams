package statemgr

import (
	"context"
	"fmt"
	"os"
)

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

// Close closes all stores and writes final checkpoint
func (sm *StateManager) Close(ctx context.Context) error {
	// Track first error to return (but continue with cleanup)
	var firstErr error

	// CRITICAL: Flush stores before checkpoint
	// Matches Kafka Streams' AbstractTask.closeStateManager()
	if err := sm.Flush(ctx); err != nil {
		sm.log.Error("Failed to flush stores before close", "error", err)
		if firstErr == nil {
			firstErr = err
		}
		// Continue with close even if flush fails
	}

	// Checkpoint after flush
	if err := sm.Checkpoint(ctx); err != nil {
		sm.log.Error("Failed to checkpoint before close", "error", err)
		if firstErr == nil {
			firstErr = err
		}
		// Continue with close even if checkpoint fails
	}

	// Close all stores
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
