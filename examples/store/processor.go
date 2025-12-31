package main

import (
	"context"
	"fmt"
	"time"

	"github.com/birdayz/kstreams"
)

func NewMyProcessor() kstreams.Processor[string, string, string, string] {
	return &MyProcessor{
		// storeName: "my-store",
	}
}

type MyProcessor struct {
	store     *kstreams.KeyValueStore[string, string]
	storeName string

	processorContext kstreams.ProcessorContext[string, string]
}

func (p *MyProcessor) Init(processorContext kstreams.ProcessorContext[string, string]) error {
	p.processorContext = processorContext
	// p.store = processorContext.GetStore(p.storeName).(*kstreams.KeyValueStore[string, string])
	return nil
}

func (p *MyProcessor) Close() error {
	return nil
}

func (p *MyProcessor) Process(ctx context.Context, k string, v string) error {
	fmt.Println("xx")
	time.Sleep(time.Second * 2)
	// old, err := p.store.Get(k)
	// if err == nil {
	// 	fmt.Println("Found old value!", k, old)
	// }
	// p.store.Set(k, v)
	// fmt.Println("New value", k, v)
	// p.processorContext.Forward(ctx, k, v)
	return nil
}

type MyProcessor2 struct {
	processorContext kstreams.ProcessorContext[string, string]
}

func (p *MyProcessor2) Process(ctx context.Context, k string, v string) error {
	fmt.Printf("Just printing out the data. Key=%s, Value=%s\n", k, v)
	p.processorContext.Forward(ctx, k, v)
	return nil
}

func (p *MyProcessor2) Init(processorContext kstreams.ProcessorContext[string, string]) error {
	p.processorContext = processorContext
	return nil
}

func (p *MyProcessor2) Close() error {
	return nil
}
