package internal

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

type Task struct {
	rootNode RecordProcessor // must be slice actually
}

func (t *Task) Process(records ...*kgo.Record) {
	for _, record := range records {
		t.rootNode.Process(record)
	}
}

func NewTask(topic string, partition int, rootNode RecordProcessor) *Task {
	return &Task{
		rootNode: rootNode,
	}
}
