package kprocessor

import (
	"context"
	"fmt"
	"time"
)

// Store is a simple interface for state stores used by processors
type Store interface {
	Init() error
	Flush() error
	Close() error
}

type ProcessorContext[Kout any, Vout any] interface {
	// Forward to all child nodes.
	//
	// Deprecated: Use RecordProcessorContext.ForwardRecord for proper timestamp
	// propagation. Forward discards timestamp metadata, which can cause
	// incorrect event-time semantics in downstream state stores and changelogs.
	Forward(ctx context.Context, k Kout, v Vout)

	// ForwardTo forwards to specific child node.
	//
	// Deprecated: Use RecordProcessorContext.ForwardRecordTo for proper timestamp
	// propagation. ForwardTo discards timestamp metadata, which can cause
	// incorrect event-time semantics in downstream state stores and changelogs.
	ForwardTo(ctx context.Context, k Kout, v Vout, childName string)

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

// MustGetStore retrieves a typed store from the context.
// Panics with a clear error message if the store is not found or has wrong type.
//
// Usage:
//
//	store := kprocessor.MustGetStore[*kstate.KeyValueStore[string, int64]](ctx, "counts")
//
// This is safer than manual type assertion:
//
//	// Before (panics with unhelpful message on type mismatch)
//	store := ctx.GetStore("counts").(*kstate.KeyValueStore[string, int64])
//
//	// After (panics with clear message)
//	store := kprocessor.MustGetStore[*kstate.KeyValueStore[string, int64]](ctx, "counts")
func MustGetStore[S Store](ctx ProcessorContext[any, any], name string) S {
	store := ctx.GetStore(name)
	if store == nil {
		panic(fmt.Sprintf("kstreams: store %q not found", name))
	}
	typed, ok := store.(S)
	if !ok {
		panic(fmt.Sprintf("kstreams: store %q is %T, expected %T", name, store, *new(S)))
	}
	return typed
}
