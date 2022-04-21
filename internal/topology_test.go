package internal

import (
	"fmt"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/streamz/sdk"
)

func TestGroup(t *testing.T) {
	top := NewTopologyBuilder()

	err := AddSource(top, "mysource", "mysource", StringDeserializer, StringDeserializer)
	assert.NoError(t, err)

	err = AddSource(top, "secondsource", "secondsource", StringDeserializer, StringDeserializer)
	assert.NoError(t, err)

	RegisterStore(top, func(partition int32) sdk.Store {
		typed := sdk.NewTypedStateStore(&InMemoryStore{
			db: map[string][]byte{},
		}, StringSerializer, StringSerializer, StringDeserializer, StringDeserializer)

		return typed
	}, "mystore")

	err = AddProcessor(top, func() sdk.Processor[string, string, string, string] {
		return &MyProcessor{}
	}, "myprocessor", "mystore")
	assert.NoError(t, err)
	err = SetParent(top, "mysource", "myprocessor")

	err = AddProcessor(top, func() sdk.Processor[string, string, string, string] {
		return &MyProcessor{}
	}, "myprocessor-2")
	assert.NoError(t, err)
	err = SetParent(top, "myprocessor", "myprocessor-2")

	err = AddProcessor(top, func() sdk.Processor[string, string, string, string] {
		return &MyProcessor{}
	}, "secondprocessor", "mystore")
	assert.NoError(t, err)
	err = SetParent(top, "secondsource", "secondprocessor")

	assert.NoError(t, err)

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
	p.store.Set(k, v)
	fmt.Println("New value", k, v)
	// ctx.Forward(k, v2)
	return nil
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

func (s *InMemoryStore) Flush() error {
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
		return nil, sdk.ErrNotFound
	}
	return res, nil
}
