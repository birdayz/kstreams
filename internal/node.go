package internal

type Nexter[K, V any] interface {
	AddNext(GenericProcessor[K, V])
}

type Process0rNode[Kin any, Vin any, Kout any, Vout any] struct {
	processor Processor[Kin, Vin, Kout, Vout]
	outputs   map[string]GenericProcessor[Kout, Vout]

	ctx *ProcessorContext[Kout, Vout]
}

func (p *Process0rNode[Kin, Vin, Kout, Vout]) Process(k Kin, v Vin) error {
	err := p.processor.Process(p.ctx, k, v)
	if err != nil {
		return err
	}

	var firstError error
	for _, err := range p.ctx.outputErrors {
		firstError = err
	}
	if firstError != nil {
		p.ctx.outputErrors = map[string]error{}
	}

	return firstError
}
