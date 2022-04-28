package internal

import (
	"context"
	"fmt"

	"github.com/birdayz/streamz/sdk"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/multierr"
)

type Task struct {
	rootNodes map[string]RecordProcessor // Key = topic

	stores []sdk.Store

	topics    []string
	partition int32

	needCommit         bool
	committableOffsets map[string]int64 // Topic => offset

	processors map[string]sdk.BaseProcessor
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
	var multierror error

	for _, processor := range t.processors {
		multierr.Append(multierror, processor.Init())
	}

	for _, store := range t.stores {
		multierr.Append(multierror, store.Init())
	}

	return multierror
}

func (t *Task) Close(ctx context.Context) error {
	var err error
	for _, store := range t.stores {
		err = multierr.Append(err, store.Close())
	}
	return err
}

func (t *Task) GetOffsetsToCommit() map[string]int64 {
	return t.committableOffsets
}

func (t *Task) ClearOffsets() {
	for k := range t.committableOffsets {
		delete(t.committableOffsets, k)
	}
}

func (t *Task) FlushStores(ctx context.Context) error {
	var errz error

	for _, store := range t.stores {
		multierr.AppendInto(&errz, store.Flush(ctx))
	}

	return errz
}

func NewTask(topics []string, partition int32, rootNodes map[string]RecordProcessor, stores []sdk.Store, processors map[string]sdk.BaseProcessor) *Task {
	return &Task{
		rootNodes:          rootNodes,
		stores:             stores,
		topics:             topics,
		partition:          partition,
		committableOffsets: map[string]int64{},
		processors:         processors,
	}
}
