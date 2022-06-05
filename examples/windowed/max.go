package main

import (
	"fmt"
	"time"

	"github.com/birdayz/kstreams/sdk"
)

func NewMaxAggregator() sdk.Processor[string, SensorData, string, float64] {
	return &MaxAggregator{}
}

type MaxAggregator struct {
	store sdk.WindowedKeyValueStore[string, float64]
}

func (p *MaxAggregator) Init(stores ...sdk.Store) error {
	if len(stores) > 0 {
		p.store = stores[0].(sdk.WindowedKeyValueStore[string, float64])
	}
	return nil
}

func (p *MaxAggregator) Close() error {
	return nil
}

// TODO make output key WindowKey[string], and generalize this
func (p *MaxAggregator) Process(ctx sdk.Context[string, float64], k string, v SensorData) error {
	// Use start of hour as timestamp
	windowStart := v.Timestamp.Truncate(time.Hour)
	state, err := p.store.Get(k, windowStart)
	if err != nil {
		state = v.Temperature
		fmt.Println("Found no state")
	}

	if v.Temperature > state {
		state = v.Temperature
	}

	// To simulate, we always set
	p.store.Set(k, state, windowStart)
	fmt.Println("New max", windowStart.String(), "is:", state)
	return nil
}
