package statemgr

import (
	"fmt"

	"github.com/birdayz/kstreams/internal/checkpoint"
)

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
// Returns error if store is not found
func (sm *StateManager) SetOffset(storeName string, offset int64) error {
	metadata, ok := sm.stores[storeName]
	if !ok {
		return fmt.Errorf("store %q not found", storeName)
	}
	metadata.Offset = &offset
	return nil
}

// UpdateChangelogOffsets updates offsets from written changelog records by TopicPartition
// Matches Kafka Streams' ProcessorStateManager.updateChangelogOffsets()
//
// Called by RecordCollector after successful changelog writes
// Updates offsets in batch for better performance than individual SetOffset() calls
func (sm *StateManager) UpdateChangelogOffsets(writtenOffsets map[checkpoint.TopicPartition]int64) {
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

// UpdateChangelogOffsetsByStore updates offsets from written changelog records by store name
// This is a convenience method for RecordCollector which tracks offsets by store name
func (sm *StateManager) UpdateChangelogOffsetsByStore(storeOffsets map[string]int64) error {
	for storeName, offset := range storeOffsets {
		if err := sm.SetOffset(storeName, offset); err != nil {
			return fmt.Errorf("update offset for store %s: %w", storeName, err)
		}
	}
	return nil
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
func (sm *StateManager) ChangelogOffsets() map[checkpoint.TopicPartition]int64 {
	offsets := make(map[checkpoint.TopicPartition]int64)

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
