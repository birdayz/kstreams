package internal

import (
	"context"
	"os"
	"runtime/pprof"
	"sync"
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
type Worker struct {
	client      *kgo.Client
	adminClient *kadm.Client
	log         *zerolog.Logger
	group       string

	t *TopologyBuilder

	state RoutineState

	assignedOrRevoked chan AssignedOrRevoked

	newlyAssigned map[string][]int32
	newlyRevoked  map[string][]int32

	closeRequested chan struct{}

	name string

	cancelPollMtx sync.Mutex
	cancelPoll    func()

	closed sync.WaitGroup

	maxPollRecords int

	taskManager *TaskManager
}

type AssignedOrRevoked struct {
	Assigned map[string][]int32
	Revoked  map[string][]int32
}

// Config
func NewWorker(name string, t *TopologyBuilder, group string, brokers []string) (*Worker, error) {
	// Need partition assignor, so we get same partition on all topics. NOT needed yet, as we do not support joins yet, state stores etc.

	// Close hangs if this channel is full/not read
	par := make(chan AssignedOrRevoked)

	topics := t.GetTopics()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		// Add balancer
		kgo.Balancers(&streamzBalancer{}),
		kgo.DisableAutoCommit(),
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

	sr := &Worker{
		name:              name,
		log:               &log,
		group:             group,
		client:            client,
		adminClient:       kadm.NewClient(client),
		t:                 t,
		state:             StateCreated,
		assignedOrRevoked: par,
		closeRequested:    make(chan struct{}, 1),
		maxPollRecords:    10000,
		taskManager: &TaskManager{
			tasks:    []*Task{},
			client:   client,
			log:      &log,
			topology: t,
			pgs:      t.partitionGroups(),
		},
	}
	sr.closed.Add(1)
	return sr, nil
}

func (r *Worker) changeState(newState RoutineState) {
	r.log.Info().Str("from", string(r.state)).Str("to", string(newState)).Msg("Change state")
	r.state = newState
}

func (r *Worker) Start() {
	go func() {
		labels := pprof.Labels("routine-name", r.name)
		pprof.Do(context.Background(), labels, func(c context.Context) {
			r.Loop()
		})
	}()
}

func (r *Worker) handleRunning() {
	r.cancelPollMtx.Lock()

	select {
	case ev := <-r.assignedOrRevoked:
		r.newlyAssigned = ev.Assigned
		r.newlyRevoked = ev.Revoked
		r.changeState(StatePartitionsAssigned)
		r.cancelPollMtx.Unlock()
		return
	default:
	}

	select {
	case <-r.closeRequested:
		r.changeState(StateCloseRequested)
		r.cancelPollMtx.Unlock()
		return
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r.cancelPoll = cancel

	r.cancelPollMtx.Unlock()

	r.log.Debug().Msg("Polling Records")
	f := r.client.PollRecords(ctx, r.maxPollRecords)
	// defer r.client.AllowRebalance()
	r.log.Debug().Msg("Polled Records")

	if f.IsClientClosed() {
		r.log.Debug().Msg("Err client Closed")
		r.changeState(StateCloseRequested)
		return
	}

	f.EachPartition(func(fetch kgo.FetchTopicPartition) {
		task, err := r.taskManager.TaskFor(fetch.Topic, fetch.Partition)
		if err != nil {
			r.log.Error().Err(err).Msg("failed to get task")
			return
		}

		r.log.Debug().Str("topic", fetch.Topic).Int32("partition", fetch.Partition).Msg("Processing")
		count := 0
		fetch.EachRecord(func(record *kgo.Record) {
			count++
			if err := task.Process(record); err != nil {
				r.log.Error().Err(err).Msg("Failed to process record")
				r.changeState(StateCloseRequested)
				return
			}
		})
		r.log.Debug().Str("topic", fetch.Topic).Int32("partition", fetch.Partition).Int("count", count).Msg("Processed")

		if err := task.Commit(r.client, r.log); err != nil {
			r.changeState(StateCloseRequested)
			return
		}

	})
}

func (r *Worker) handleClosed() {
	r.closed.Done()
}

func (r *Worker) handlePartitionsAssigned() {
	if err := r.taskManager.Revoked(r.newlyRevoked); err != nil {
		r.log.Error().Err(err).Msg("revoked failed")
	}

	if err := r.taskManager.Assigned(r.newlyAssigned); err != nil {
		r.log.Error().Err(err).Msg("assigned failed")
	}

	r.newlyAssigned = nil
	r.newlyRevoked = nil

	if len(r.taskManager.tasks) > 0 {
		r.changeState(StateRunning)
	} else {
		r.changeState(StateCreated)
	}
}

// State transitions may only be done from within the loop
func (r *Worker) Loop() {
	for {
		switch r.state {
		case StateClosed:
			r.handleClosed()

			// Done, exit loop
			return
		case StateCloseRequested:

			wg := sync.WaitGroup{}
			wg.Add(1)

			go func() {
				// Wait until this is closed
				for range r.assignedOrRevoked {
					// TODO: OnRevoke: do commit here if needed
				}
				wg.Done()
			}()

			err := r.taskManager.Close()
			if err != nil {
				r.log.Error().Err(err).Msg("Failed to close tasks")
			}

			r.client.Close()
			close(r.assignedOrRevoked) // Can close this only after client is closed, as the client writer to that channel
			wg.Wait()
			r.changeState(StateClosed)
		case StateCreated:
			select {
			case assignments := <-r.assignedOrRevoked:
				r.newlyAssigned = assignments.Assigned
				r.newlyRevoked = assignments.Revoked
				r.changeState(StatePartitionsAssigned)
				continue
			case <-r.closeRequested:
				r.changeState(StateCloseRequested)
				continue
			}

		// In State PartitionsAssigned, tasks are set up. it transists to RUNNING
		// once all tasks are ready
		case StatePartitionsAssigned:
			r.handlePartitionsAssigned()
		case StateRunning:
			r.handleRunning()
		}

	}
}

func (r *Worker) Close() error {
	r.log.Debug().Msg("Close")

	r.log.Debug().Msg("cancel")
	r.cancelPollMtx.Lock()

	r.closeRequested <- struct{}{}
	if r.cancelPoll != nil {
		r.cancelPoll()
	}

	r.cancelPollMtx.Unlock()
	r.log.Debug().Msg("cancelled")

	r.closed.Wait()

	r.log.Debug().Msg("Closed")
	return nil
}

type TopicPartition struct {
	Topic     string
	Partition int32
}
