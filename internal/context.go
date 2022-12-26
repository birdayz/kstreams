package internal

import (
	"context"
)

type ProcessorContext[Kout any, Vout any] struct {
	context.Context
	outputs      map[string]GenericProcessor[Kout, Vout]
	outputErrors []error
}

func (c *ProcessorContext[Kout, Vout]) Forward(k Kout, v Vout, processorNames ...string) {
	if len(processorNames) == 0 {
		for _, p := range c.outputs {
			if err := p.Process(c, k, v); err != nil {
				c.outputErrors = append(c.outputErrors, err)
			}
		}
	} else {
		for _, name := range processorNames {
			if p, ok := c.outputs[name]; ok {
				if err := p.Process(c, k, v); err != nil {
					c.outputErrors = append(c.outputErrors, err)
				}
			}
		}
	}
}
