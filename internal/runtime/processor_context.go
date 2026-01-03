package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/birdayz/kstreams/internal/checkpoint"
	"github.com/birdayz/kstreams/kprocessor"
)

// StateManager interface for managing state stores.
// Defined here to avoid circular dependency with statemgr package.
type StateManager interface {
	ChangelogPartitionFor(storeName string) *checkpoint.TopicPartition
	SetOffset(storeName string, offset int64) error
}

// NodeID is a string identifier for nodes
type NodeID string

// NewInternalProcessorContext creates a new internal processor context
func NewInternalProcessorContext[Kout any, Vout any](
	outputs map[string]InputProcessor[Kout, Vout],
	stores map[string]kprocessor.Store,
) *InternalProcessorContext[Kout, Vout] {
	return &InternalProcessorContext[Kout, Vout]{
		outputs: outputs,
		stores:  stores,
	}
}

// NewInternalProcessorContextWithState creates a new internal processor context with state manager
func NewInternalProcessorContextWithState[Kout any, Vout any](
	outputs map[string]InputProcessor[Kout, Vout],
	stores map[string]kprocessor.Store,
	stateManager StateManager,
	collector ChangelogCollector,
) *InternalProcessorContext[Kout, Vout] {
	return &InternalProcessorContext[Kout, Vout]{
		outputs:      outputs,
		stores:       stores,
		stateManager: stateManager,
		collector:    collector,
	}
}

// InternalProcessorContext provides processor context for record processing.
//
// THREAD SAFETY: This type is NOT thread-safe and does not use mutexes.
// This is intentional and safe because of the Kafka Streams processing model:
//   - Each partition is processed by exactly one task
//   - Each task runs in a single goroutine
//   - Each processor instance is bound to exactly one partition/task
//
// Therefore, there is no concurrent access to any InternalProcessorContext instance.
// This matches Kafka Streams' ProcessorContextImpl which is also not thread-safe.
type InternalProcessorContext[Kout any, Vout any] struct {
	outputs      map[string]InputProcessor[Kout, Vout]
	stores       map[string]kprocessor.Store
	outputErrors []error

	// Changelog support
	stateManager StateManager
	collector    ChangelogCollector // Buffers changelog records for batch produce

	// Current record timestamp for changelog writes
	// CRITICAL: Must use record timestamp (not wall clock) for event-time semantics
	// Updated before processing each record
	currentTimestamp time.Time
}

func (c *InternalProcessorContext[Kout, Vout]) drainErrors() []error {
	if len(c.outputErrors) == 0 {
		return nil
	}
	// Copy and clear errors
	errs := make([]error, len(c.outputErrors))
	copy(errs, c.outputErrors)
	c.outputErrors = c.outputErrors[:0]
	return errs
}

func (c *InternalProcessorContext[Kout, Vout]) Forward(ctx context.Context, k Kout, v Vout) {
	for name, p := range c.outputs {
		if err := p.Process(ctx, k, v); err != nil {
			c.outputErrors = append(c.outputErrors, fmt.Errorf("failed to forward record to node %s: %w", name, err))
		}
	}
}

func (c *InternalProcessorContext[Kout, Vout]) ForwardTo(ctx context.Context, k Kout, v Vout, childName string) {
	p, ok := c.outputs[childName]
	if !ok {
		c.outputErrors = append(c.outputErrors, fmt.Errorf("child processor %q not found", childName))
		return
	}
	if err := p.Process(ctx, k, v); err != nil {
		c.outputErrors = append(c.outputErrors, fmt.Errorf("failed to forward to %s: %w", childName, err))
	}
}

func (c *InternalProcessorContext[Kout, Vout]) GetStore(name string) kprocessor.Store {
	return c.stores[name]
}

// LogChange logs a state change to the changelog topic
// Implements ProcessorContextInternal interface
//
// This method buffers the changelog record for batch production during flush.
// Offsets are updated after the batch is produced (during Task.Flush).
func (c *InternalProcessorContext[Kout, Vout]) LogChange(storeName string, key, value []byte) error {
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
		// Fallback to wall clock if timestamp not set (shouldn't happen in normal processing)
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

// clearErrors clears accumulated errors (called before processing each record)
func (c *InternalProcessorContext[Kout, Vout]) clearErrors() {
	c.outputErrors = nil
}

// InternalRecordProcessorContext implements RecordProcessorContext
type InternalRecordProcessorContext[Kout any, Vout any] struct {
	*InternalProcessorContext[Kout, Vout] // Embed old

	punctuationMgr *PunctuationManager
	taskID         string
	partition      int32
	currentRecord  kprocessor.RecordMetadata
}

// AddOutput adds a downstream processor (delegates to embedded context)
func (c *InternalRecordProcessorContext[Kout, Vout]) AddOutput(childID NodeID, child InputProcessor[Kout, Vout]) {
	// Delegate to embedded context if it exists
	// For now, this is a no-op since we're embedding InternalProcessorContext
	// which has outputs as a map[string] not map[NodeID]
}

// ForwardRecord forwards a record with full metadata to all child nodes.
// The record's timestamp is propagated so downstream processors and state stores
// use the correct event time for changelog writes.
func (c *InternalRecordProcessorContext[Kout, Vout]) ForwardRecord(ctx context.Context, record kprocessor.Record[Kout, Vout]) {
	// Propagate timestamp to ensure downstream changelog writes use correct event time
	c.currentTimestamp = record.Metadata.Timestamp
	c.currentRecord = record.Metadata
	c.Forward(ctx, record.Key, record.Value)
}

// ForwardRecordTo forwards a record with full metadata to a specific child node.
// The record's timestamp is propagated so downstream processors and state stores
// use the correct event time for changelog writes.
func (c *InternalRecordProcessorContext[Kout, Vout]) ForwardRecordTo(ctx context.Context, record kprocessor.Record[Kout, Vout], childName string) {
	// Propagate timestamp to ensure downstream changelog writes use correct event time
	c.currentTimestamp = record.Metadata.Timestamp
	c.currentRecord = record.Metadata
	c.ForwardTo(ctx, record.Key, record.Value, childName)
}

// StreamTime returns the current stream time (max timestamp seen)
func (c *InternalRecordProcessorContext[Kout, Vout]) StreamTime() time.Time {
	if c.punctuationMgr == nil {
		return time.Time{}
	}
	return c.punctuationMgr.StreamTime()
}

// WallClockTime returns the current wall clock time
func (c *InternalRecordProcessorContext[Kout, Vout]) WallClockTime() time.Time {
	return time.Now()
}

// RecordMetadata returns the metadata for the current record being processed
func (c *InternalRecordProcessorContext[Kout, Vout]) RecordMetadata() kprocessor.RecordMetadata {
	return c.currentRecord
}

// TaskID returns the task ID
func (c *InternalRecordProcessorContext[Kout, Vout]) TaskID() string {
	return c.taskID
}

// Partition returns the partition number
func (c *InternalRecordProcessorContext[Kout, Vout]) Partition() int32 {
	return c.partition
}

// errCancellable is a no-op Cancellable returned when punctuation manager is not initialized
type errCancellable struct{}

func (e *errCancellable) Cancel() {}

// Schedule adds a new punctuation schedule
func (c *InternalRecordProcessorContext[Kout, Vout]) Schedule(
	interval time.Duration,
	pType kprocessor.PunctuationType,
	callback kprocessor.Punctuator,
) kprocessor.Cancellable {
	if c.punctuationMgr == nil {
		// Return a no-op cancellable instead of panicking
		// This allows the processor to continue even if punctuation is not configured
		// The callback will simply never be invoked
		return &errCancellable{}
	}
	return c.punctuationMgr.schedule(interval, pType, callback)
}

// Headers returns the headers for the current record
func (c *InternalRecordProcessorContext[Kout, Vout]) Headers() *kprocessor.Headers {
	return c.currentRecord.Headers
}
