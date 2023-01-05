package processors

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/birdayz/kstreams"
)

type WindowedAggregator[Kin, Vin, State, Vout any] struct {
	store *WindowedKeyValueStore[Kin, State]

	// TODO make optional. if not given, or returns no timestamp, use
	// record timestamp
	timestampExtractor func(Kin, Vin) time.Time
	windowSize         time.Duration
	initFunc           func() State
	aggregateFunc      func(Vin, State) State
	finalizeFunc       func(State) Vout
	storeName          string
	processorContext   kstreams.ProcessorContext[WindowKey[Kin], Vout]
}

func NewWindowedAggregator[Kin, Vin, State, Vout any](
	timestampExtractor func(Kin, Vin) time.Time,
	windowSize time.Duration,
	initFunc func() State,
	aggregateFunc func(Vin, State) State,
	finalizeFunc func(State) Vout,

	// For store
	storeBackendBuilder kstreams.StoreBackendBuilder,
	keySerde kstreams.SerDe[Kin],
	stateSerde kstreams.SerDe[State],
	storeName string,
) (
	kstreams.ProcessorBuilder[Kin, Vin, WindowKey[Kin], Vout],
	kstreams.StoreBuilder,
) {
	processorBuilder := func() kstreams.Processor[Kin, Vin, WindowKey[Kin], Vout] {
		return &WindowedAggregator[Kin, Vin, State, Vout]{
			timestampExtractor: timestampExtractor,
			windowSize:         windowSize,
			initFunc:           initFunc,
			aggregateFunc:      aggregateFunc,
			finalizeFunc:       finalizeFunc,
			storeName:          storeName,
		}
	}

	storeBuilder := WindowedStore(storeBackendBuilder, keySerde, stateSerde)

	return processorBuilder, storeBuilder
}

// TODO change output Key to WindowKey[Kin]
func (p *WindowedAggregator[Kin, Vin, State, Vout]) Process(ctx context.Context, k Kin, v Vin) error {
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

	p.processorContext.Forward(ctx, WindowKey[Kin]{
		Key:  k,
		Time: ts,
	}, p.finalizeFunc(state))

	return nil
}

func (p *WindowedAggregator[Kin, Vin, State, Vout]) Init(processorContext kstreams.ProcessorContext[WindowKey[Kin], Vout]) error {
	p.processorContext = processorContext
	p.store = processorContext.GetStore(p.storeName).(*WindowedKeyValueStore[Kin, State])
	return nil
}

func (p *WindowedAggregator[Kin, Vin, State, Vout]) Close() error {
	return nil
}

type WindowedKeyValueStore[K, V any] struct {
	store                 kstreams.StoreBackend
	windowKeySerializer   kstreams.Serializer[WindowKey[K]]
	valueSerializer       kstreams.Serializer[V]
	windowKeyDeserializer kstreams.Deserializer[WindowKey[K]]
	valueDeserializer     kstreams.Deserializer[V]
}

func NewWindowedKeyValueStore[K, V any](
	store kstreams.StoreBackend,
	keySerializer kstreams.Serializer[K],
	valueSerializer kstreams.Serializer[V],
	windowKeyDeserializer kstreams.Deserializer[K],
	valueDeserializer kstreams.Deserializer[V],
) *WindowedKeyValueStore[K, V] {
	return &WindowedKeyValueStore[K, V]{
		store:                 store,
		windowKeySerializer:   WindowKeySerializer(keySerializer),
		valueSerializer:       valueSerializer,
		windowKeyDeserializer: WindowKeyDeserializer(windowKeyDeserializer),
		valueDeserializer:     valueDeserializer,
	}
}

func (s *WindowedKeyValueStore[K, V]) Set(k K, v V, t time.Time) error {
	wk := WindowKey[K]{
		Key:  k,
		Time: t,
	}
	keyBytes, err := s.windowKeySerializer(wk)
	if err != nil {
		return err
	}

	valueBytes, err := s.valueSerializer(v)
	if err != nil {
		return err
	}

	return s.store.Set(keyBytes, valueBytes)
}

func (s *WindowedKeyValueStore[K, V]) Get(k K, t time.Time) (V, error) {
	var v V

	wk := WindowKey[K]{
		Key:  k,
		Time: t,
	}

	key, err := s.windowKeySerializer(wk)
	if err != nil {
		return v, err
	}

	res, err := s.store.Get(key)
	if err != nil {
		return v, err
	}

	return s.valueDeserializer(res)
}

func (t *WindowedKeyValueStore[K, V]) Init() error {
	return t.store.Init()
}

func (t *WindowedKeyValueStore[K, V]) Flush(ctx context.Context) error {
	return t.store.Flush(ctx)
}

func (t *WindowedKeyValueStore[K, V]) Close() error {
	return t.store.Close()
}

func WindowedStore[K, V any](storeBuilder func(name string, p int32) (kstreams.StoreBackend, error), keySerde kstreams.SerDe[K], valueSerde kstreams.SerDe[V]) func(name string, p int32) (kstreams.Store, error) {
	return func(name string, p int32) (kstreams.Store, error) {
		backend, err := storeBuilder(name, p)
		if err != nil {
			return nil, err
		}

		return NewWindowedKeyValueStore(backend, keySerde.Serializer, valueSerde.Serializer, keySerde.Deserializer, valueSerde.Deserializer), nil

	}
}

type WindowKey[K any] struct {
	Key  K
	Time time.Time
}

// TODO: consider making these internal again. this representation, is it useful
// for anybody?
func WindowKeySerializer[K any](serializer kstreams.Serializer[K]) kstreams.Serializer[WindowKey[K]] {
	return func(wk WindowKey[K]) ([]byte, error) {
		buf := bytes.NewBuffer(nil)

		// It might be interesting, if serializers are not just functions but
		// interfaces, and can optionally implement "MarshalTo", which directly
		// writes to an io.Writer.
		serializedKey, err := serializer(wk.Key)
		if err != nil {
			return nil, err
		}

		lnPrefix := make([]byte, 2)
		binary.BigEndian.PutUint16(lnPrefix, uint16(len(serializedKey))) // TODO careful
		if _, err := buf.Write(lnPrefix); err != nil {
			return nil, err
		}

		if _, err := buf.Write(serializedKey); err != nil {
			return nil, err
		}

		ts, err := wk.Time.MarshalBinary()
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(ts); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}
}

func WindowKeyDeserializer[K any](deserializer kstreams.Deserializer[K]) kstreams.Deserializer[WindowKey[K]] {
	return func(b []byte) (key WindowKey[K], err error) {
		length := binary.BigEndian.Uint16(b)
		if len(b) < int(length)+1+8 {
			return WindowKey[K]{}, fmt.Errorf("eof")
		}

		b = b[2:]

		deserialized, err := deserializer(b[:length])
		if err != nil {
			return WindowKey[K]{}, err
		}

		b = b[length:]

		var t time.Time
		err = t.UnmarshalBinary(b)
		if err != nil {
			return WindowKey[K]{}, err
		}

		return WindowKey[K]{
			Key:  deserialized,
			Time: t,
		}, nil
	}
}
