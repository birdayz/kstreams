package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/birdayz/kstreams/kprocessor"
)

// IsolatedProcessorContext maintains errors PER NODE
// This fixes the critical bug where errors from node A were appearing in node B
// Each processor instance gets its own context with its own error slice
type IsolatedProcessorContext[Kout, Vout any] struct {
	nodeID  NodeID
	outputs map[NodeID]InputProcessor[Kout, Vout]
	stores  map[string]kprocessor.Store

	// CRITICAL: This is per-instance, not shared!
	// Each RuntimeProcessorNode has its own IsolatedProcessorContext
	errorsMutex sync.Mutex
	errors      []error

	// Changelog support
	stateManager StateManager
	collector    ChangelogCollector // Buffers changelog records for batch produce

	// Current record timestamp for changelog writes
	// CRITICAL: Must use record timestamp (not wall clock) for event-time semantics
	currentTimestamp time.Time
}

// NewIsolatedProcessorContext creates a new isolated processor context
func NewIsolatedProcessorContext[Kout, Vout any](
	nodeID NodeID,
	stores map[string]kprocessor.Store,
	stateManager StateManager,
	collector ChangelogCollector,
) *IsolatedProcessorContext[Kout, Vout] {
	return &IsolatedProcessorContext[Kout, Vout]{
		nodeID:       nodeID,
		outputs:      make(map[NodeID]InputProcessor[Kout, Vout]),
		stores:       stores,
		errors:       make([]error, 0),
		stateManager: stateManager,
		collector:    collector,
	}
}

// AddOutput adds a downstream processor to this context
func (c *IsolatedProcessorContext[Kout, Vout]) AddOutput(childID NodeID, child InputProcessor[Kout, Vout]) {
	c.outputs[childID] = child
}

// Forward sends the key/value to all downstream processors
// Any errors during forwarding are captured in THIS node's error slice only
func (c *IsolatedProcessorContext[Kout, Vout]) Forward(ctx context.Context, k Kout, v Vout) {
	// Collect errors locally first, then lock once to append
	// This avoids acquiring/releasing the mutex for each error in the hot path
	var localErrs []error
	for nodeID, proc := range c.outputs {
		if err := proc.Process(ctx, k, v); err != nil {
			localErrs = append(localErrs, fmt.Errorf("forward to %s: %w", nodeID, err))
		}
	}
	if len(localErrs) > 0 {
		c.errorsMutex.Lock()
		c.errors = append(c.errors, localErrs...)
		c.errorsMutex.Unlock()
	}
}

// ForwardTo sends the key/value to a specific downstream processor by name
// This is used when a processor wants to route records selectively
func (c *IsolatedProcessorContext[Kout, Vout]) ForwardTo(ctx context.Context, k Kout, v Vout, childName string) {
	nodeID := NodeID(childName)
	proc, ok := c.outputs[nodeID]
	if !ok {
		c.errorsMutex.Lock()
		c.errors = append(c.errors, fmt.Errorf("child %s not found", childName))
		c.errorsMutex.Unlock()
		return
	}

	if err := proc.Process(ctx, k, v); err != nil {
		c.errorsMutex.Lock()
		c.errors = append(c.errors, fmt.Errorf("forward to %s: %w", nodeID, err))
		c.errorsMutex.Unlock()
	}
}

// GetStore retrieves a state store by name
func (c *IsolatedProcessorContext[Kout, Vout]) GetStore(name string) kprocessor.Store {
	return c.stores[name]
}

// clearErrors resets the error slice before each Process() invocation
// This ensures errors from previous records don't leak into the current one
func (c *IsolatedProcessorContext[Kout, Vout]) clearErrors() {
	c.errorsMutex.Lock()
	c.errors = c.errors[:0] // Keep capacity, just reset length
	c.errorsMutex.Unlock()
}

// drainErrors retrieves all errors that occurred during this Process() invocation
// and clears the error slice for the next invocation
func (c *IsolatedProcessorContext[Kout, Vout]) drainErrors() []error {
	c.errorsMutex.Lock()
	defer c.errorsMutex.Unlock()

	if len(c.errors) == 0 {
		return nil
	}

	// Copy errors to return
	errs := make([]error, len(c.errors))
	copy(errs, c.errors)

	// Clear for next invocation
	c.errors = c.errors[:0]

	return errs
}

// LogChange logs a state change to the changelog topic
// Implements ProcessorContextInternal interface
//
// This method buffers the changelog record for batch production during flush.
// Offsets are updated after the batch is produced (during Task.Flush).
func (c *IsolatedProcessorContext[Kout, Vout]) LogChange(storeName string, key, value []byte) error {
	if c.stateManager == nil {
		// No state manager (changelog not configured)
		return fmt.Errorf("no state manager configured for changelog logging")
	}

	if c.collector == nil {
		// No collector (changelog not configured)
		return fmt.Errorf("no changelog collector configured")
	}

	// Get changelog partition from state manager
	changelogPartition := c.stateManager.ChangelogPartitionFor(storeName)
	if changelogPartition == nil {
		return fmt.Errorf("no changelog partition for store: %s", storeName)
	}

	// Use record timestamp for event-time correctness
	// This ensures changelog records preserve event-time semantics
	// Matches Kafka Streams' RecordCollector.send(... context.timestamp() ...)
	timestamp := c.currentTimestamp
	if timestamp.IsZero() {
		// Fallback to wall clock if timestamp not set
		timestamp = time.Now()
	}

	// Buffer the record for batch production
	// Actual produce happens during Task.Flush() -> RecordCollector.Flush()
	// Offsets are updated after batch produce completes
	c.collector.Send(
		storeName,
		changelogPartition.Topic,
		changelogPartition.Partition,
		key,
		value,
		timestamp,
	)

	return nil
}

// Verify that IsolatedProcessorContext implements kprocessor interfaces
// Commented out for now - these need proper imports
// var _ kprocessor.ProcessorContext[any, any] = (*IsolatedProcessorContext[any, any])(nil)
// var _ kprocessor.ProcessorContextInternal = (*IsolatedProcessorContext[any, any])(nil)
