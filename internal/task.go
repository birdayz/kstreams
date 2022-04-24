package internal

import (
	"context"
	"fmt"

	"github.com/birdayz/streamz/sdk"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

func (t *Task) Process(records ...*kgo.Record) error {
	for _, record := range records {

		p, ok := t.rootNodes[record.Topic]
		if !ok {
			return fmt.Errorf("unknown topic: %s", record.Topic)
		}

		if err := p.Process(record); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
		t.committableOffsets[record.Topic] = record.Offset + 1
		if !t.needCommit {
			t.needCommit = true
		}
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

func (t *Task) Close() error {
	var err error
	for _, store := range t.stores {
		err = multierr.Append(err, store.Close())
	}
	return err
}

func (t *Task) Commit(client *kgo.Client) error {
	if t.needCommit {
		errCh := make(chan error, 1)

		commitData := map[string]map[int32]kgo.EpochOffset{}

		for topic, offset := range t.committableOffsets {
			commitData[topic] = map[int32]kgo.EpochOffset{
				t.partition: {Offset: offset},
			}
		}

		client.CommitOffsetsSync(context.Background(), commitData, func(c *kgo.Client, ocr1 *kmsg.OffsetCommitRequest, ocr2 *kmsg.OffsetCommitResponse, e error) {
			if e != nil {
				errCh <- e
				return
			}

			for _, t := range ocr2.Topics {
				for _, p := range t.Partitions {
					err := kerr.ErrorForCode(p.ErrorCode)
					if err != nil {
						errCh <- err
						return
					}
				}
			}

			errCh <- nil

		})

		if err := <-errCh; err != nil {
			return err
		}

		// FIXME check for errors
		t.needCommit = false
	}
	return nil
}

// TODO: move state store init here ? so init is here, and close is managed by task as well.
func NewTask(topics []string, partition int32, rootNodes map[string]RecordProcessor, stores []sdk.Store, processors map[string]sdk.BaseProcessor) *Task {
	// TODO init procs with stores ?
	return &Task{
		rootNodes:          rootNodes,
		stores:             stores,
		topics:             topics,
		partition:          partition,
		committableOffsets: map[string]int64{},
		processors:         processors,
	}
}
