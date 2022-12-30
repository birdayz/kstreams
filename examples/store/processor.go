package main

import (
	"fmt"

	"github.com/birdayz/kstreams"
)

func NewMyProcessor() kstreams.Processor[string, string, string, string] {
	return &MyProcessor{}
}

type MyProcessor struct {
	store *kstreams.KeyValueStore[string, string]
}

func (p *MyProcessor) Init(stores ...kstreams.Store) error {
	if len(stores) > 0 {
		p.store = stores[0].(*kstreams.KeyValueStore[string, string])
	}
	return nil
}

func (p *MyProcessor) Close() error {
	return nil
}

func (p *MyProcessor) Process(ctx kstreams.Context[string, string], k string, v string) error {
	old, err := p.store.Get(k)
	if err == nil {
		fmt.Println("Found old value!", k, old)
	}
	p.store.Set(k, v)
	fmt.Println("New value", k, v)
	ctx.Forward(k, v)
	return nil
}

type MyProcessor2 struct{}

func (p *MyProcessor2) Process(ctx kstreams.Context[string, string], k string, v string) error {
	fmt.Printf("Just printing out the data. Key=%s, Value=%s\n", k, v)
	ctx.Forward(k, v)
	return nil
}

func (p *MyProcessor2) Init(stores ...kstreams.Store) error {
	return nil
}

func (p *MyProcessor2) Close() error {
	return nil
}
