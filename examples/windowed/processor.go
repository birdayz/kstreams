package main

import (
	"fmt"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/processors"
)

type WindowState struct {
	Count  int
	Values []float64
}

func NewAverageAggregator() kstreams.Processor[string, SensorData, string, float64] {
	return &AvgAggregator{}
}

type AvgAggregator struct {
	store *processors.WindowedKeyValueStore[string, WindowState]
}

func (p *AvgAggregator) Init(stores ...kstreams.Store) error {
	if len(stores) > 0 {
		p.store = stores[0].(*processors.WindowedKeyValueStore[string, WindowState])
	}
	return nil
}

func (p *AvgAggregator) Close() error {
	return nil
}

// TODO make output key WindowKey[string], and generalize this
func (p *AvgAggregator) Process(ctx kstreams.Context[string, float64], k string, v SensorData) error {
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
