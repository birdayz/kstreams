package internal

import (
	"context"
	"fmt"

	"github.com/birdayz/streamz/sdk"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.uber.org/multierr"
)

type Task struct {
	rootNodes map[string]RecordProcessor // Key = topic

	stores []sdk.Store

	topic     string
	partition int32

	needCommit       bool
	comittableOffset int64
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
		t.comittableOffset = record.Offset + 1
		if !t.needCommit {
			t.needCommit = true
		}
	}

	return nil
}

func (t *Task) Close() error {
	var err error
	for _, store := range t.stores {
		err = multierr.Append(err, store.Close())
	}
	return err
}

func (t *Task) Commit(client *kgo.Client, log *zerolog.Logger) error {
	if t.needCommit {
		errCh := make(chan error, 1)
		client.CommitOffsetsSync(context.Background(), map[string]map[int32]kgo.EpochOffset{
			t.topic: {
				t.partition: {
					Offset: t.comittableOffset,
				},
			},
		}, func(c *kgo.Client, ocr1 *kmsg.OffsetCommitRequest, ocr2 *kmsg.OffsetCommitResponse, e error) {
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

		log.Info().Str("topic", t.topic).Int32("partition", t.partition).Int64("offset", t.comittableOffset).Msg("Committed")
	}
	return nil
}

// TODO: move state store init here ? so init is here, and close is managed by task as well.
func NewTask(topic string, partition int32, rootNodes map[string]RecordProcessor, stores []sdk.Store) *Task {
	return &Task{
		rootNodes: rootNodes,
		stores:    stores,
		topic:     topic,
		partition: partition,
	}
}
