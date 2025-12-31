package kstreams

import (
	"context"
	"errors"
)

// ProcessorNodeBatch wraps a BatchProcessor for use in the topology.
type ProcessorNodeBatch[Kin, Vin, Kout, Vout any] struct {
	userProcessor    BatchProcessor[Kin, Vin, Kout, Vout]
	processorContext *InternalProcessorContext[Kout, Vout]
}

// Ensure ProcessorNodeBatch implements both single and batch interfaces
var _ InputProcessor[string, string] = (*ProcessorNodeBatch[string, string, string, string])(nil)
var _ BatchInputProcessor[string, string] = (*ProcessorNodeBatch[string, string, string, string])(nil)

func NewProcessorNodeBatch[Kin, Vin, Kout, Vout any](
	userProcessor BatchProcessor[Kin, Vin, Kout, Vout],
	processorContext *InternalProcessorContext[Kout, Vout],
) *ProcessorNodeBatch[Kin, Vin, Kout, Vout] {
	return &ProcessorNodeBatch[Kin, Vin, Kout, Vout]{
		userProcessor:    userProcessor,
		processorContext: processorContext,
	}
}

// Process implements single-record processing (fallback).
func (p *ProcessorNodeBatch[Kin, Vin, Kout, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
	// Clear previous errors
	p.processorContext.outputErrors = nil

	// Convert to Record and call ProcessBatch with size 1
	record := Record[Kin, Vin]{Key: k, Value: v}
	if err := p.userProcessor.ProcessBatch(ctx, []Record[Kin, Vin]{record}); err != nil {
		return err
	}

	// Check for forwarding errors
	if errs := p.processorContext.drainErrors(); len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// ProcessBatch implements batch processing.
func (p *ProcessorNodeBatch[Kin, Vin, Kout, Vout]) ProcessBatch(ctx context.Context, keys []Kin, values []Vin) error {
	// Clear previous errors
	p.processorContext.outputErrors = nil

	// Convert to Record slice
	records := make([]Record[Kin, Vin], len(keys))
	for i := range keys {
		records[i] = Record[Kin, Vin]{
			Key:   keys[i],
			Value: values[i],
		}
	}

	// Call user's batch processor
	if err := p.userProcessor.ProcessBatch(ctx, records); err != nil {
		return err
	}

	// Check for forwarding errors
	if errs := p.processorContext.drainErrors(); len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

// Init initializes the processor.
func (p *ProcessorNodeBatch[Kin, Vin, Kout, Vout]) Init() error {
	return p.userProcessor.Init(p.processorContext)
}

// Close closes the processor.
func (p *ProcessorNodeBatch[Kin, Vin, Kout, Vout]) Close() error {
	return p.userProcessor.Close()
}

// InternalBatchProcessorContext extends InternalProcessorContext with batch operations.
type InternalBatchProcessorContext[Kout, Vout any] struct {
	*InternalProcessorContext[Kout, Vout]
}

// Ensure it implements BatchProcessorContext
var _ BatchProcessorContext[string, string] = (*InternalBatchProcessorContext[string, string])(nil)

// ForwardBatch forwards multiple key-value pairs to all downstream processors.
func (c *InternalBatchProcessorContext[Kout, Vout]) ForwardBatch(ctx context.Context, records []KV[Kout, Vout]) error {
	if len(records) == 0 {
		return nil
	}

	for _, proc := range c.outputs {
		// Check if downstream supports batch processing
		if batchProc, ok := proc.(BatchInputProcessor[Kout, Vout]); ok {
			// Extract keys and values for batch processing
			keys := make([]Kout, len(records))
			values := make([]Vout, len(records))
			for i, kv := range records {
				keys[i] = kv.Key
				values[i] = kv.Value
			}

			if err := batchProc.ProcessBatch(ctx, keys, values); err != nil {
				c.outputErrors = append(c.outputErrors, err)
			}
		} else {
			// Fallback: forward one-by-one
			for _, kv := range records {
				if err := proc.Process(ctx, kv.Key, kv.Value); err != nil {
					c.outputErrors = append(c.outputErrors, err)
				}
			}
		}
	}

	if len(c.outputErrors) > 0 {
		return errors.Join(c.outputErrors...)
	}

	return nil
}

// ForwardBatchTo forwards multiple key-value pairs to a specific downstream processor.
func (c *InternalBatchProcessorContext[Kout, Vout]) ForwardBatchTo(
	ctx context.Context,
	records []KV[Kout, Vout],
	childName string,
) error {
	if len(records) == 0 {
		return nil
	}

	proc, ok := c.outputs[childName]
	if !ok {
		return nil // Silently ignore if child not found
	}

	// Check if downstream supports batch processing
	if batchProc, ok := proc.(BatchInputProcessor[Kout, Vout]); ok {
		// Extract keys and values for batch processing
		keys := make([]Kout, len(records))
		values := make([]Vout, len(records))
		for i, kv := range records {
			keys[i] = kv.Key
			values[i] = kv.Value
		}

		if err := batchProc.ProcessBatch(ctx, keys, values); err != nil {
			c.outputErrors = append(c.outputErrors, err)
		}
	} else {
		// Fallback: forward one-by-one
		for _, kv := range records {
			if err := proc.Process(ctx, kv.Key, kv.Value); err != nil {
				c.outputErrors = append(c.outputErrors, err)
			}
		}
	}

	if len(c.outputErrors) > 0 {
		return errors.Join(c.outputErrors...)
	}

	return nil
}
