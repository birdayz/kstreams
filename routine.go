package streamz

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Nice read https://jaceklaskowski.gitbooks.io/mastering-kafka-streams/content/kafka-streams-internals-StreamThread.html
type StreamRoutine struct {
	client *kgo.Client
	// Franz client

	// Sources
	Sources []SourceFunc

	Tasks map[TopicPartition]*Task

	// Partition assign0r
}

func (r *StreamRoutine) Loop() {
	for {
		r.client.PollFetches(context.TODO())

		// Poll from source(s)

	}
}

func (r *StreamRoutine) Close() error {
	return nil
}

type TopicPartition struct {
	Topic string
}

func LinkSource[K, V, K2, V2 any](source *Source[K, V], firstProcessor *Processor[K, V, K2, V2]) {
	source.forwardFunc = func(k K, v V) {
		firstProcessor.Process(k, v)
	}
}
