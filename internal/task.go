package internal

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Task struct {
	rootNode RecordProcessor // must be slice actually

	topic     string
	partition int32

	needCommit       bool
	comittableOffset int64
}

func (t *Task) Process(records ...*kgo.Record) error {
	for _, record := range records {
		if err := t.rootNode.Process(record); err != nil {
			return fmt.Errorf("failed to process record: %w", err)
		}
		t.comittableOffset = record.Offset + 1
		if !t.needCommit {
			t.needCommit = true
		}
	}

	return nil
}

func (t *Task) Commit(client *kgo.Client, log *zerolog.Logger) error {
	if t.needCommit {
		client.CommitOffsetsSync(context.Background(), map[string]map[int32]kgo.EpochOffset{
			t.topic: {
				t.partition: {
					Offset: t.comittableOffset,
				},
			},
		}, nil)
		// FIXME check for errors
		t.needCommit = false

		log.Info().Str("topic", t.topic).Int32("partition", t.partition).Int64("offset", t.comittableOffset).Msg("Committed")
	}
	return nil
}

func NewTask(topic string, partition int32, rootNode RecordProcessor) *Task {
	return &Task{
		rootNode:  rootNode,
		topic:     topic,
		partition: partition,
	}
}
