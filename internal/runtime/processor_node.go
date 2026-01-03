package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/birdayz/kstreams/kprocessor"
)

// RuntimeProcessorNode is the runtime instantiation of a processor node
// It maintains full type safety with isolated error handling
// Supports both legacy Processor and new RecordProcessor interfaces
type RuntimeProcessorNode[Kin, Vin, Kout, Vout any] struct {
	id              string // NodeID is just a string
	userProcessor   kprocessor.Processor[Kin, Vin, Kout, Vout] //nolint:staticcheck // SA1019: backward compat
	recordProcessor kprocessor.RecordProcessor[Kin, Vin, Kout, Vout]
	context         *IsolatedProcessorContext[Kout, Vout]
	recordContext   *InternalRecordProcessorContext[Kout, Vout]
}

// NewRuntimeProcessorNode creates a new runtime processor node
//
//nolint:staticcheck // SA1019: Processor kept for backward compatibility
func NewRuntimeProcessorNode[Kin, Vin, Kout, Vout any](
	id string,
	processor kprocessor.Processor[Kin, Vin, Kout, Vout],
	context *IsolatedProcessorContext[Kout, Vout],
) *RuntimeProcessorNode[Kin, Vin, Kout, Vout] {
	return &RuntimeProcessorNode[Kin, Vin, Kout, Vout]{
		id:            id,
		userProcessor: processor,
		context:       context,
	}
}

// Process invokes the user processor with full type safety and isolated error handling
// This is for legacy processors that receive bare key-value pairs
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	// Clear errors from previous invocation - THIS IS THE FIX!
	n.context.clearErrors()

	// Call user processor
	if err := n.userProcessor.Process(ctx, k, v); err != nil {
		return fmt.Errorf("node %s: user processor: %w", n.id, err)
	}

	// Get THIS node's errors only - no shared state!
	if errs := n.context.drainErrors(); len(errs) > 0 {
		return fmt.Errorf("node %s: forward errors: %w", n.id, errors.Join(errs...))
	}

	return nil
}

// ProcessRecord invokes the RecordProcessor with full metadata
// This implements RecordInputProcessor for enhanced processors
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) ProcessRecord(ctx context.Context, record kprocessor.Record[Kin, Vin]) error {
	if n.recordProcessor != nil {
		// Clear errors from previous invocation
		if n.recordContext != nil {
			n.recordContext.clearErrors()
		}

		// Update context with current record metadata
		if n.recordContext != nil {
			n.recordContext.currentRecord = record.Metadata
			// CRITICAL: Also update timestamp in embedded InternalProcessorContext
			// This ensures changelog writes use record timestamp (not wall clock)
			// Matches Kafka Streams' behavior for event-time semantics
			n.recordContext.currentTimestamp = record.Metadata.Timestamp
		}

		// Call enhanced processor
		if err := n.recordProcessor.ProcessRecord(ctx, record); err != nil {
			return fmt.Errorf("node %s: record processor: %w", n.id, err)
		}

		// Check for forwarding errors
		if n.recordContext != nil {
			if errs := n.recordContext.drainErrors(); len(errs) > 0 {
				return fmt.Errorf("node %s: forward errors: %w", n.id, errors.Join(errs...))
			}
		}

		return nil
	}

	// Fallback to legacy processor with bare key-value
	return n.Process(ctx, record.Key, record.Value)
}

// SetCurrentTimestamp sets the current timestamp in the processor context
// CRITICAL: This must be called before Process() for legacy processors
// to ensure changelog writes use the correct event timestamp
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) SetCurrentTimestamp(timestamp time.Time) {
	if n.context != nil {
		n.context.currentTimestamp = timestamp
	}
	if n.recordContext != nil {
		n.recordContext.currentTimestamp = timestamp
	}
}

// Init initializes the processor by calling the user processor's Init
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) Init() error {
	if n.recordProcessor != nil {
		return n.recordProcessor.Init(n.recordContext)
	}
	return n.userProcessor.Init(n.context)
}

// Close closes the processor
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) Close() error {
	if n.recordProcessor != nil {
		return n.recordProcessor.Close()
	}
	return n.userProcessor.Close()
}

// AddOutput adds a downstream processor to this node's context
// This is used during topology wiring
func (n *RuntimeProcessorNode[Kin, Vin, Kout, Vout]) AddOutput(childID string, child InputProcessor[Kout, Vout]) {
	if n.context != nil {
		n.context.AddOutput(NodeID(childID), child)
	}
	if n.recordContext != nil {
		n.recordContext.AddOutput(NodeID(childID), child)
	}
}

// Verify that RuntimeProcessorNode implements Node, InputProcessor, and RecordInputProcessor
var _ Node = (*RuntimeProcessorNode[any, any, any, any])(nil)
var _ InputProcessor[any, any] = (*RuntimeProcessorNode[any, any, any, any])(nil)
var _ RecordInputProcessor[any, any] = (*RuntimeProcessorNode[any, any, any, any])(nil)
