package internal

import "context"

type ProcessorContext[Kout any, Vout any] struct {
	ctx     context.Context
	outputs map[string]GenericProcessor[Kout, Vout]

	outputErrors map[string]error
}

func (c *ProcessorContext[Kout, Vout]) Forward(k Kout, v Vout) {
	for name, p := range c.outputs {
		if err := p.Process(c.ctx, k, v); err != nil {
			c.outputErrors[name] = err
		}
	}
}
