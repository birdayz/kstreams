package internal

import (
	"context"
	"fmt"

	"github.com/birdayz/kstreams/sdk"
	"github.com/hashicorp/go-multierror"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Task struct {
	rootNodes map[string]RecordProcessor // Key = topic

	stores map[string]sdk.Store

	topics    []string
	partition int32

	committableOffsets map[string]int64 // Topic => offset

	processors map[string]sdk.BaseProcessor

	sinks              map[string]Flusher
	processorsToStores map[string][]string
}

func (t *Task) Process(ctx context.Context, records ...*kgo.Record) error {
	for _, record := range records {

		p, ok := t.rootNodes[record.Topic]
		if !ok {
			return fmt.Errorf("unknown topic: %s", record.Topic)
		}

		if err := p.Process(ctx, record); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
		t.committableOffsets[record.Topic] = record.Offset + 1
	}

	return nil
}

func (t *Task) Init() error {
	var err *multierror.Error

	for processorName, processor := range t.processors {
		var stores []sdk.Store
		for _, store := range t.processorsToStores[processorName] {
			stores = append(stores, t.stores[store])

		}
		err = multierror.Append(err, processor.Init(stores...))
	}

	for _, store := range t.stores {
		err = multierror.Append(err, store.Init())
	}

	return err.ErrorOrNil()
}

func (t *Task) Close(ctx context.Context) error {
	var err *multierror.Error
	for _, store := range t.stores {
		err = multierror.Append(err, store.Close())
	}
	return err.ErrorOrNil()
}

func (t *Task) GetOffsetsToCommit() map[string]int64 {
	return t.committableOffsets
}

func (t *Task) ClearOffsets() {
	for k := range t.committableOffsets {
		delete(t.committableOffsets, k)
	}
}

// Flush flushes state stores and sinks.
func (t *Task) Flush(ctx context.Context) error {
	var err *multierror.Error

	for _, store := range t.stores {
		err = multierror.Append(err, store.Flush(ctx))
	}

	for _, sink := range t.sinks {
		err = multierror.Append(err, sink.Flush(ctx))
	}

	return err.ErrorOrNil()
}

func (t *Task) String() string {
	return fmt.Sprintf("%v-%d", t.topics, t.partition)
}

func NewTask(topics []string, partition int32, rootNodes map[string]RecordProcessor, stores map[string]sdk.Store, processors map[string]sdk.BaseProcessor, sinks map[string]Flusher, processorToStore map[string][]string) *Task {
	return &Task{
		rootNodes:          rootNodes,
		stores:             stores,
		topics:             topics,
		partition:          partition,
		committableOffsets: map[string]int64{},
		processors:         processors,
		sinks:              sinks,
		processorsToStores: processorToStore,
	}
}
