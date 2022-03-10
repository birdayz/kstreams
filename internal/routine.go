package internal

import (
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"time"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type RoutineState string

const (
	StateCreated            = "CREATED"
	StatePartitionsAssigned = "PARTITIONS_ASSIGNED"
	StateRunning            = "RUNNING"
	StateCloseRequested     = "CLOSE_REQUESTED"
	StateClosed             = "CLOSED"
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

	assignedOrRevoked chan AssignedOrRevoked

	partitionsAssigned map[string][]int32

	closeRequested chan struct{}

	name string
}

type AssignedOrRevoked struct {
	Assigned map[string][]int32
	Revoked  map[string][]int32
}

// Config
func NewStreamRoutine(name string, t *TopologyBuilder, group string, brokers []string) (*StreamRoutine, error) {
	// Need partition assignor, so we get same partition on all topics. NOT needed yet, as we do not support joins yet, state stores etc.

	// Close hangs if this channel is full/not read
	par := make(chan AssignedOrRevoked)

	topics := t.GetTopics()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.OnPartitionsAssigned(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			par <- AssignedOrRevoked{Assigned: m}
		}),
		kgo.OnPartitionsRevoked(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			par <- AssignedOrRevoked{Revoked: m}
		}),
	)
	if err != nil {
		return nil, err
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02T15:04:05.999Z07:00"}
	log := zerolog.New(output).With().Timestamp().Logger().With().Str("component", name).Logger()

	return &StreamRoutine{
		name:              name,
		log:               &log,
		group:             group,
		client:            client,
		adminClient:       kadm.NewClient(client),
		Tasks:             map[TopicPartition]*Task{},
		t:                 t,
		state:             StateCreated,
		assignedOrRevoked: par,
	}, nil
}

func (r *StreamRoutine) changeState(newState RoutineState) {
	r.log.Info().Str("from", string(r.state)).Str("to", string(newState)).Msg("Change state")
	r.state = newState
}

func (r *StreamRoutine) Start() {
	go func() {
		labels := pprof.Labels("routine-name", r.name)
		pprof.Do(context.Background(), labels, func(c context.Context) {
			r.Loop()
		})
	}()
}

// State transitions may only be done from within the loop
func (r *StreamRoutine) Loop() {
	for {
		switch r.state {
		case StateClosed:
			return
		case StateCloseRequested:
			r.client.Close()
			r.changeState(StateClosed)
		case StateCreated:
			select {
			case assignments := <-r.assignedOrRevoked:
				if len(assignments.Assigned) > 0 {
					r.changeState(StatePartitionsAssigned)
					r.partitionsAssigned = assignments.Assigned
				}
				continue
			case <-r.closeRequested:
				r.changeState(StateCloseRequested)
				continue
			}

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

			// Assigned!

			select {
			case x := <-r.assignedOrRevoked:
				_ = x
				r.log.Info().Msg("Assigned or rev p.")
			default:
			}

			//r.client.AllowRebalance()

			// TODO: partitions assigned can be called even when in state running!

			// Check if partitions have been revoked

			// FIXME If client is stuck here, it can't consume from "partitions revoked" signal
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
			f := r.client.PollFetches(ctx)

			if f.IsClientClosed() {

				fmt.Println("clozer")
				r.changeState(StateClosed)
				cancel()
				continue
			}

			f.EachPartition(func(ftp kgo.FetchTopicPartition) {
				r.log.Debug().Str("topic", ftp.Topic).Int32("partition", ftp.Partition).Msg("Process Msg")

				tp := TopicPartition{
					Topic:     ftp.Topic,
					Partition: ftp.Partition,
				}

				// TODO Can actually happen, before we get the info that partions have been assigned
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
			cancel()
		}

	}
}

func (r *StreamRoutine) Close() error {
	r.log.Debug().Msg("Close")
	r.m.Lock()
	r.exit = true
	r.m.Unlock()

	r.log.Debug().Msg("Client close")
	r.client.Close()

	r.log.Debug().Msg("Client closed")
	r.log.Debug().Msg("Closed")
	return nil
}

type TopicPartition struct {
	Topic     string
	Partition int32
}
