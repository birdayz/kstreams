package main

import (
	"fmt"
	"time"

	"github.com/birdayz/kstreams/sdk"
)

type WindowState struct {
	Count  int
	Values []float64
}

func NewMyProcessor() sdk.Processor[string, SensorData, string, float64] {
	return &MyProcessor{}
}

type MyProcessor struct {
	store sdk.WindowedKeyValueStore[string, WindowState]
}

func (p *MyProcessor) Init(stores ...sdk.Store) error {
	if len(stores) > 0 {
		p.store = stores[0].(sdk.WindowedKeyValueStore[string, WindowState])
	}
	return nil
}

func (p *MyProcessor) Close() error {
	return nil
}

// TODO make output key WindowKey[string], and generalize this
func (p *MyProcessor) Process(ctx sdk.Context[string, float64], k string, v SensorData) error {
	// Use start of hour as timestamp
	windowStart := v.Timestamp.Truncate(time.Hour)
	state, err := p.store.Get(k, windowStart)
	if err != nil {
		fmt.Println("Found no state")
	}
	state.Count++
	state.Values = append(state.Values, v.Temperature)
	p.store.Set(k, state, windowStart)
	var sum float64

	for _, val := range state.Values {
		sum += val
	}
	fmt.Println("New average of window", windowStart.String(), "is:", sum/float64(state.Count))
	return nil
}
