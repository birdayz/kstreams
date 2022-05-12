package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/streamz/sdk"
)

func sampleTopology(t *testing.T) *TopologyBuilder {
	top := NewTopologyBuilder()

	err := AddSource(top, "mysource", "mysource", StringDeserializer, StringDeserializer)
	assert.NoError(t, err)

	err = AddSource(top, "secondsource", "secondsource", StringDeserializer, StringDeserializer)
	assert.NoError(t, err)

	RegisterStore(top, func(name string, partition int32) (sdk.Store, error) {
		typed := newKVStore(&InMemoryStore{
			db: map[string][]byte{},
		}, StringSerializer, StringSerializer, StringDeserializer, StringDeserializer)

		return typed, nil
	}, "mystore")

	err = AddProcessor(top, func() sdk.Processor[string, string, string, string] {
		return &MyProcessor{}
	}, "myprocessor", "mystore")
	assert.NoError(t, err)
	err = SetParent(top, "mysource", "myprocessor")
	assert.NoError(t, err)

	err = AddProcessor(top, func() sdk.Processor[string, string, string, string] {
		return &MyProcessor{}
	}, "myprocessor-2")
	assert.NoError(t, err)
	err = SetParent(top, "myprocessor", "myprocessor-2")
	assert.NoError(t, err)

	err = AddProcessor(top, func() sdk.Processor[string, string, string, string] {
		return &MyProcessor{}
	}, "secondprocessor", "mystore")
	assert.NoError(t, err)
	err = SetParent(top, "secondsource", "secondprocessor")
	assert.NoError(t, err)

	err = AddSink(top, "sink-topic", "sink-topic", StringSerializer, StringSerializer)
	assert.NoError(t, err)

	err = SetParent(top, "myprocessor-2", "sink-topic")
	assert.NoError(t, err)

	return top
}

func TestCreateTask(t *testing.T) {
	top := sampleTopology(t)

	task, err := top.CreateTask([]string{"mysource"}, 0, nil)
	assert.NoError(t, err)
	_ = task
}

func TestGroup(t *testing.T) {
	top := sampleTopology(t)

	assert.Equal(t, map[string][]string{"myprocessor": {"mystore"}, "secondprocessor": {"mystore"}, "myprocessor-2": nil}, top.processorToStores)

	pgs := top.partitionGroups()

	assert.Equal(t, []*PartitionGroup{{
		sourceTopics:   []string{"mysource", "secondsource"},
		processorNames: []string{"myprocessor", "myprocessor-2", "secondprocessor"},
		storeNames:     []string{"mystore"},
	}}, pgs)

}

type MyProcessor struct {
	store sdk.KeyValueStore[string, string]
}

func (p *MyProcessor) Init(stores ...sdk.Store) error {
	if len(stores) > 0 {
		p.store = stores[0].(sdk.KeyValueStore[string, string])
	}
	return nil
}

func (p *MyProcessor) Close() error {
	return nil
}

func (p *MyProcessor) Process(ctx sdk.Context[string, string], k string, v string) error {
	// v2 := v + "-modified"
	old, err := p.store.Get(k)
	if err == nil {
		fmt.Println("Found old value!", k, old)
	}
	err = p.store.Set(k, v)
	fmt.Println("New value", k, v)
	// ctx.Forward(k, v2)
	return err
}

var StringDeserializer = func(data []byte) (string, error) {
	return string(data), nil
}

var StringSerializer = func(data string) ([]byte, error) {
	return []byte(data), nil
}

type InMemoryStore struct {
	db map[string][]byte
}

func (s *InMemoryStore) Init() error {
	return nil
}

func (s *InMemoryStore) Flush(ctx context.Context) error {
	return nil
}

func (s *InMemoryStore) Close() error {
	return nil
}

func (s *InMemoryStore) Set(k, v []byte) error {
	s.db[string(k)] = v
	return nil
}

func (s *InMemoryStore) Get(k []byte) ([]byte, error) {
	res, ok := s.db[string(k)]
	if !ok {
		return nil, errors.New("not found")
	}
	return res, nil
}

func newKVStore[K, V any](
	store sdk.StoreBackend,
	keySerializer sdk.Serializer[K],
	valueSerializer sdk.Serializer[V],
	keyDeserializer sdk.Deserializer[K],
	valueDeserializer sdk.Deserializer[V],
) sdk.KeyValueStore[K, V] {
	return &keyValueStore[K, V]{
		store:             store,
		keySerializer:     keySerializer,
		valueSerializer:   valueSerializer,
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
	}
}

type keyValueStore[K, V any] struct {
	store             sdk.StoreBackend
	keySerializer     sdk.Serializer[K]
	valueSerializer   sdk.Serializer[V]
	keyDeserializer   sdk.Deserializer[K]
	valueDeserializer sdk.Deserializer[V]
}

func (t *keyValueStore[K, V]) Init() error {
	return t.store.Init()
}

func (t *keyValueStore[K, V]) Flush(ctx context.Context) error {
	return t.store.Flush(ctx)
}

func (t keyValueStore[K, V]) Close() error {
	return t.store.Close()
}

func (t *keyValueStore[K, V]) Set(k K, v V) error {
	key, err := t.keySerializer(k)
	if err != nil {
		return err
	}

	value, err := t.valueSerializer(v)
	if err != nil {
		return err
	}

	return t.store.Set(key, value)
}

func (t *keyValueStore[K, V]) Get(k K) (V, error) {
	var v V
	key, err := t.keySerializer(k)
	if err != nil {
		return v, err
	}

	res, err := t.store.Get(key)
	if err != nil {
		return v, err
	}

	return t.valueDeserializer(res)
}
