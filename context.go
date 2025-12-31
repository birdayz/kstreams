package kstreams

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ProcessorContext[Kout any, Vout any] interface {
	// Forward to all child nodes.
	Forward(ctx context.Context, k Kout, v Vout)
	// Forward to specific child node. Panics if child node is not found.
	ForwardTo(ctx context.Context, k Kout, v Vout, childName string) // TBD: should forward return error ? or are errs...ignored?
	// Get state store by name. Returns nil if not found.
	GetStore(name string) Store
}

// ProcessorContextInternal extends ProcessorContext with internal methods
// Used by changelog stores to log changes to changelog topics
// NOT exposed to user processors
type ProcessorContextInternal interface {
	// LogChange logs a state change to the changelog topic
	// Matches Kafka Streams' ProcessorContextImpl.logChange()
	//
	// Parameters:
	//   - storeName: Name of the state store
	//   - key: Serialized key bytes
	//   - value: Serialized value bytes (nil for tombstone)
	//
	// This method:
	//  1. Looks up changelog partition from StateManager
	//  2. Produces record to changelog topic with correct partition
	//  3. Uses current record's timestamp (or wall clock if no record context)
	LogChange(storeName string, key, value []byte) error
}

// RecordProcessorContext is an enhanced processor context with full metadata access
// It embeds ProcessorContext for backward compatibility
type RecordProcessorContext[Kout any, Vout any] interface {
	ProcessorContext[Kout, Vout] // Embed for compatibility

	// Record forwarding with metadata
	ForwardRecord(ctx context.Context, record Record[Kout, Vout])
	ForwardRecordTo(ctx context.Context, record Record[Kout, Vout], childName string)

	// Time tracking
	StreamTime() time.Time
	WallClockTime() time.Time

	// Metadata access
	RecordMetadata() RecordMetadata
	TaskID() string
	Partition() int32

	// Punctuation
	Schedule(interval time.Duration, pType PunctuationType, callback Punctuator) Cancellable

	// Headers for current record
	Headers() *Headers
}

func NewInternalkProcessorContext[Kout any, Vout any](
	outputs map[string]InputProcessor[Kout, Vout],
	stores map[string]Store,
) *InternalProcessorContext[Kout, Vout] {
	return &InternalProcessorContext[Kout, Vout]{}
}

type InternalProcessorContext[Kout any, Vout any] struct {
	outputs      map[string]InputProcessor[Kout, Vout]
	stores       map[string]Store
	outputErrors []error

	// Changelog support
	stateManager *StateManager
	client       *kgo.Client

	// Current record timestamp for changelog writes
	// CRITICAL: Must use record timestamp (not wall clock) for event-time semantics
	// Updated before processing each record
	currentTimestamp time.Time
}

func (c *InternalProcessorContext[Kout, Vout]) drainErrors() []error {
	res := c.outputErrors
	return res
}

func (c *InternalProcessorContext[Kout, Vout]) Forward(ctx context.Context, k Kout, v Vout) {
	for name, p := range c.outputs {
		if err := p.Process(ctx, k, v); err != nil {
			c.outputErrors = append(c.outputErrors, fmt.Errorf("failed to forward record to node %s: %w", name, err))
		}
	}
}

func (c *InternalProcessorContext[Kout, Vout]) ForwardTo(ctx context.Context, k Kout, v Vout, childName string) {
	if p, ok := c.outputs[childName]; ok {
		if err := p.Process(ctx, k, v); err != nil {
			c.outputErrors = append(c.outputErrors, err)
		}
	}
}

func (c *InternalProcessorContext[Kout, Vout]) GetStore(name string) Store {
	return c.stores[name]
}

// LogChange logs a state change to the changelog topic
// Implements ProcessorContextInternal interface
func (c *InternalProcessorContext[Kout, Vout]) LogChange(storeName string, key, value []byte) error {
	if c.stateManager == nil {
		// No state manager (changelog not configured)
		return fmt.Errorf("no state manager configured for changelog logging")
	}

	if c.client == nil {
		// No client (changelog not configured)
		return fmt.Errorf("no kafka client configured for changelog logging")
	}

	// Get changelog partition from state manager
	changelogPartition := c.stateManager.ChangelogPartitionFor(storeName)
	if changelogPartition == nil {
		return fmt.Errorf("no changelog partition for store: %s", storeName)
	}

	// Create changelog record
	// CRITICAL: Use record timestamp (not wall clock) for event-time correctness
	// This ensures changelog records preserve event-time semantics
	// Matches Kafka Streams' RecordCollector.send(... context.timestamp() ...)
	timestamp := c.currentTimestamp
	if timestamp.IsZero() {
		// Fallback to wall clock if timestamp not set (shouldn't happen in normal processing)
		timestamp = time.Now()
	}

	record := &kgo.Record{
		Topic:     changelogPartition.Topic,
		Partition: changelogPartition.Partition,
		Key:       key,
		Value:     value, // nil for tombstone
		Timestamp: timestamp,
	}

	// Produce to changelog topic synchronously
	// (Could be optimized to async with callback)
	results := c.client.ProduceSync(context.Background(), record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("produce to changelog topic: %w", err)
	}

	// CRITICAL: Update offset in StateManager after successful produce
	// This ensures checkpoints reflect the latest changelog offset
	// Matches Kafka Streams' ProcessorStateManager.updateChangelogOffsets()
	for _, result := range results {
		if result.Err == nil {
			c.stateManager.SetOffset(storeName, result.Record.Offset)
		}
	}

	return nil
}

// clearErrors clears accumulated errors (called before processing each record)
func (c *InternalProcessorContext[Kout, Vout]) clearErrors() {
	c.outputErrors = nil
}

// InternalRecordProcessorContext implements RecordProcessorContext
type InternalRecordProcessorContext[Kout any, Vout any] struct {
	*InternalProcessorContext[Kout, Vout] // Embed old

	punctuationMgr *punctuationManager
	taskID         string
	partition      int32
	currentRecord  RecordMetadata
}

// ForwardRecord forwards a record with full metadata to all child nodes
func (c *InternalRecordProcessorContext[Kout, Vout]) ForwardRecord(ctx context.Context, record Record[Kout, Vout]) {
	// For now, fallback to forwarding just key-value
	// Full record forwarding will be implemented when runtime nodes support it
	c.Forward(ctx, record.Key, record.Value)
}

// ForwardRecordTo forwards a record with full metadata to a specific child node
func (c *InternalRecordProcessorContext[Kout, Vout]) ForwardRecordTo(ctx context.Context, record Record[Kout, Vout], childName string) {
	// For now, fallback to forwarding just key-value
	// Full record forwarding will be implemented when runtime nodes support it
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
func (c *InternalRecordProcessorContext[Kout, Vout]) RecordMetadata() RecordMetadata {
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

// Schedule adds a new punctuation schedule
func (c *InternalRecordProcessorContext[Kout, Vout]) Schedule(
	interval time.Duration,
	pType PunctuationType,
	callback Punctuator,
) Cancellable {
	if c.punctuationMgr == nil {
		panic("punctuation manager not initialized")
	}
	return c.punctuationMgr.schedule(interval, pType, callback)
}

// Headers returns the headers for the current record
func (c *InternalRecordProcessorContext[Kout, Vout]) Headers() *Headers {
	return c.currentRecord.Headers
}
