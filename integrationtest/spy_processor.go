package integrationtest

import "github.com/birdayz/kstreams"

type SpyProcessor struct {
	out chan [2]string
}

func (p *SpyProcessor) Init(stores ...kstreams.Store) error {
	return nil
}

func (p *SpyProcessor) Close() error {
	return nil
}

func (p *SpyProcessor) Process(ctx kstreams.Context[string, string], k string, v string) error {
	p.out <- [...]string{k, v}
	return nil
}
