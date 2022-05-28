package integrationtest

import "github.com/birdayz/kstreams/sdk"

type SpyProcessor struct {
	out chan [2]string
}

func (p *SpyProcessor) Init(stores ...sdk.Store) error {
	return nil
}

func (p *SpyProcessor) Close() error {
	return nil
}

func (p *SpyProcessor) Process(ctx sdk.Context[string, string], k string, v string) error {
	p.out <- [...]string{k, v}
	return nil
}
