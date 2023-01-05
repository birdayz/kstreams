package kstreams

import (
	"context"
	"fmt"
)

type ProcessorContext[Kout any, Vout any] interface {
	// Forward to all child nodes.
	Forward(ctx context.Context, k Kout, v Vout)
	// Forward to specific child node. Panics if child node is not found.
	ForwardTo(ctx context.Context, k Kout, v Vout, childName string) // TBD: should forward return error ? or are errs...ignored?
	// Get state store by name. Returns nil if not found.
	GetStore(name string) Store
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
