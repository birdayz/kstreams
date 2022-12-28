package processors

import (
	"errors"
	"fmt"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/sdk"
)

type WindowedAggregator[Kin, Vin, State, Vout any] struct {
	store sdk.WindowedKeyValueStore[Kin, State]

	// TODO make optional. if not given, or returns no timestamp, use
	// record timestamp
	timestampExtractor func(Kin, Vin) time.Time
	windowSize         time.Duration
	initFunc           func() State
	aggregateFunc      func(Vin, State) State
	finalizeFunc       func(State) Vout
}

func NewWindowedAggregator[Kin, Vin, State, Vout any](
	timestampExtractor func(Kin, Vin) time.Time,
	windowSize time.Duration,
	initFunc func() State,
	aggregateFunc func(Vin, State) State,
	finalizeFunc func(State) Vout,

	// For store
	storeBackendBuilder sdk.StoreBackendBuilder,
	keySerde sdk.SerDe[Kin],
	stateSerde sdk.SerDe[State],
) (
	sdk.ProcessorBuilder[Kin, Vin, sdk.WindowKey[Kin], Vout],
	sdk.StoreBuilder,
) {
	processorBuilder := func() sdk.Processor[Kin, Vin, sdk.WindowKey[Kin], Vout] {
		return &WindowedAggregator[Kin, Vin, State, Vout]{
			timestampExtractor: timestampExtractor,
			windowSize:         windowSize,
			initFunc:           initFunc,
			aggregateFunc:      aggregateFunc,
			finalizeFunc:       finalizeFunc,
		}
	}

	storeBuilder := kstreams.WindowedStore(storeBackendBuilder, keySerde, stateSerde)

	return processorBuilder, storeBuilder
}

// TODO change output Key to WindowKey[Kin]
func (p *WindowedAggregator[Kin, Vin, State, Vout]) Process(ctx sdk.Context[sdk.WindowKey[Kin], Vout], k Kin, v Vin) error {
	ts := p.timestampExtractor(k, v).Truncate(p.windowSize)
	state, err := p.store.Get(k, ts)
	if err != nil {
		if errors.Is(err, kstreams.ErrKeyNotFound) {
			state = p.initFunc()
		} else {
			return err
		}
	}

	state = p.aggregateFunc(v, state)
	if err := p.store.Set(k, state, ts); err != nil {
		return err
	}

	ctx.Forward(sdk.WindowKey[Kin]{
		Key:  k,
		Time: ts,
	}, p.finalizeFunc(state))

	return nil
}

func (p *WindowedAggregator[Kin, Vin, State, Vout]) Init(stores ...sdk.Store) error {
	if len(stores) > 0 {
		p.store = stores[0].(sdk.WindowedKeyValueStore[Kin, State])
	} else {
		return fmt.Errorf("init: expected state store to be injected, but got none")
	}
	return nil
}

func (p *WindowedAggregator[Kin, Vin, State, Vout]) Close() error {
	return nil
}
