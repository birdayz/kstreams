package internal

import (
	"context"

	"github.com/birdayz/kstreams/sdk"
)

type ContextKeyRecordMetadata struct{}

func SetRecordMetadata(ctx context.Context, meta sdk.RecordMetadata) context.Context {
	return context.WithValue(ctx, ContextKeyRecordMetadata{}, meta)
}

func GetRecordMetadata(ctx context.Context) sdk.RecordMetadata {
	return ctx.Value(ContextKeyRecordMetadata{}).(sdk.RecordMetadata)
}

type ProcessorContext[Kout any, Vout any] struct {
	context.Context
	outputs      map[string]GenericProcessor[Kout, Vout]
	outputErrors []error

	topic     string
	partition int32
	offset    int64
}

func (c *ProcessorContext[Kout, Vout]) RecordMetadata() sdk.RecordMetadata {
	return GetRecordMetadata(c.Context)
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
