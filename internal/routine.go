package internal

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RoutineState string

const (
	StateCreated            = "CREATED"
	StatePartitionsAssigned = "PARTITIONS_ASSIGNED"
	StateRunning            = "RUNNING"
)

// Nice read https://jaceklaskowski.gitbooks.io/mastering-kafka-streams/content/kafka-streams-internals-StreamThread.html
type StreamRoutine struct {
	client      *kgo.Client
	adminClient *kadm.Client
	log         *zerolog.Logger
	group       string

	Tasks map[TopicPartition]*Task

	t *TopologyBuilder

	state RoutineState

	partitionsAssignedSignal chan map[string][]int32
	partitionsRevokedSignal  chan map[string][]int32

	partitionsAssigned map[string][]int32

	m    sync.Mutex
	exit bool
}

// Config
func NewStreamRoutine(t *TopologyBuilder, group string, brokers []string) (*StreamRoutine, error) {

	// Need partition assignor, so we get same partition on all topics. NOT needed yet, as we do not support joins yet, state stores etc.

	pa := make(chan map[string][]int32, 1)
	pr := make(chan map[string][]int32, 1)

	topics := t.GetTopics()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.OnPartitionsAssigned(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			pa <- m
		}),
		kgo.OnPartitionsRevoked(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			pr <- m
		}),
	)
	if err != nil {
		return nil, err
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02T15:04:05.999Z07:00"}
	log := zerolog.New(output).With().Timestamp().Logger()

	return &StreamRoutine{
		log:                      &log,
		group:                    group,
		client:                   client,
		adminClient:              kadm.NewClient(client),
		Tasks:                    map[TopicPartition]*Task{},
		t:                        t,
		state:                    StateCreated,
		partitionsAssignedSignal: pa,
		partitionsRevokedSignal:  pr,
	}, nil
}

func (r *StreamRoutine) changeState(newState RoutineState) {
	r.log.Info().Str("new", string(newState)).Str("old", string(r.state)).Msg("Change state")
	r.state = newState
}

func (r *StreamRoutine) Start() {
	go r.Loop()
}

// State transitions may only be done from within the loop
func (r *StreamRoutine) Loop() {
	for {

		r.m.Lock()
		exit := r.exit
		r.m.Unlock()
		if exit {
			return
		}

		switch r.state {
		case StateCreated:
			assignments := <-r.partitionsAssignedSignal
			r.changeState(StatePartitionsAssigned)
			r.partitionsAssigned = assignments
			continue
		case StatePartitionsAssigned:
			for topic, partitions := range r.partitionsAssigned {
				for _, partition := range partitions {
					tp := TopicPartition{
						Topic:     topic,
						Partition: partition,
					}
					task, err := r.t.CreateTask(tp)
					if err != nil {
						panic(err)
					}

					r.Tasks[tp] = task
					r.log.Debug().Str("topic", tp.Topic).Int32("partition", tp.Partition).Msg("Created Task")
				}

			}
			r.changeState(StateRunning)
			continue
		case StateRunning:

			select {
			case x := <-r.partitionsRevokedSignal:
				_ = x
				log.Info().Msg("Lost partitions")
			default:
			}

			// TODO: partitions assigned can be called even when in state running!

			// Check if partitions have been revoked

			f := r.client.PollFetches(context.TODO())

			if f.IsClientClosed() {
				r.changeState("DEAD")
				continue
			}

			f.EachPartition(func(ftp kgo.FetchTopicPartition) {

				tp := TopicPartition{
					Topic:     ftp.Topic,
					Partition: ftp.Partition,
				}

				task, ok := r.Tasks[tp]
				if !ok {
					panic("task not found")
				}

				ftp.EachRecord(func(rec *kgo.Record) {
					task.Process(rec)

					// TODO: do not commit every time
					err := r.client.CommitRecords(context.Background(), rec)

					r.log.Info().Err(err).Msg("Commit rec")

				})

			})
		}

	}
}

func (r *StreamRoutine) Close() error {
	r.m.Lock()
	r.exit = true
	r.m.Unlock()

	r.client.Close()
	// TODO Wait until exited
	return nil
}

type TopicPartition struct {
	Topic     string
	Partition int32
}
