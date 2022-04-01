package internal

type ProcessorContext[Kout any, Vout any] struct {
	outputs map[string]GenericProcessor[Kout, Vout]

	outputErrors map[string]error
}

func (c *ProcessorContext[Kout, Vout]) Forward(k Kout, v Vout) {
	for name, p := range c.outputs {
		if err := p.Process(k, v); err != nil {
			c.outputErrors[name] = err
		}
	}
}
