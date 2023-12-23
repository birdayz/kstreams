package kstreams

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Task struct {
	rootNodes map[string]RecordProcessor // Key = topic

	stores map[string]Store

	topics    []string
	partition int32

	committableOffsets map[string]kgo.EpochOffset // Topic => offset

	processors map[string]Node

	sinks              map[string]Flusher
	processorsToStores map[string][]string
}

func NewTask(topics []string, partition int32, rootNodes map[string]RecordProcessor, stores map[string]Store, processors map[string]Node, sinks map[string]Flusher, processorToStore map[string][]string) *Task {
	// spew.Dump(processorToStore)
	return &Task{
		rootNodes:          rootNodes,
		stores:             stores,
		topics:             topics,
		partition:          partition,
		committableOffsets: map[string]kgo.EpochOffset{},
		processors:         processors,
		sinks:              sinks,
		processorsToStores: processorToStore,
	}
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
		t.committableOffsets[record.Topic] = kgo.EpochOffset{Epoch: record.LeaderEpoch, Offset: record.Offset + 1}
	}

	return nil
}

func (t *Task) Init() error {
	var err *multierror.Error

	for _, processor := range t.processors {
		err = multierror.Append(err, processor.Init())
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

func (t *Task) GetOffsetsToCommit() map[string]kgo.EpochOffset {
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
		err = multierror.Append(err, store.Flush())
	}

	for _, sink := range t.sinks {
		err = multierror.Append(err, sink.Flush(ctx))
	}

	return err.ErrorOrNil()
}

func (t *Task) String() string {
	return fmt.Sprintf("%v-%d", t.topics, t.partition)
}
