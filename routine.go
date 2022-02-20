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
	//	Sources []SourceFunc

	Tasks map[TopicPartition]*Task

	t *TopologyBuilder

	// Partition assign0r
}

func NewStreamRoutine(client *kgo.Client, t *TopologyBuilder) *StreamRoutine {
	return &StreamRoutine{
		client: client,
		Tasks:  map[TopicPartition]*Task{},
		t:      t,
	}
}

func (r *StreamRoutine) Loop() {
	for {
		f := r.client.PollFetches(context.TODO())

		f.EachPartition(func(ftp kgo.FetchTopicPartition) {

			tp := TopicPartition{
				Topic:     ftp.Topic,
				Partition: ftp.Partition,
			}

			task, ok := r.Tasks[tp]
			if !ok {
				// get new task
				t, err := r.t.CreateTask(tp)
				if err != nil {
					panic(err)
				}
				r.Tasks[tp] = t
				task = t
			}

			ftp.EachRecord(func(r *kgo.Record) {
				task.Process(r)
			})

		})

		// Poll from source(s)

	}
}

func (r *StreamRoutine) Close() error {
	return nil
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

// TBD: StreamThread: one per partition? one/configurable (like kstreams) ?, each has its own consumer?

// TODO
// Processor exists once per partition. Java design. Can we do better, or is it the best?

func LinkSource[K, V, K2, V2 any](source *Source[K, V], firstProcessor *Processor[K, V, K2, V2]) {
	source.forwardFunc = func(k K, v V) {
		firstProcessor.Process(k, v)
	}
}
