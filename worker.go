package kstreams

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
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
	log         *slog.Logger
	group       string

	t *Topology

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

	lastSuccessfulCommit time.Time
	commitInterval       time.Duration

	err error
}

type AssignedOrRevoked struct {
	Assigned map[string][]int32
	Revoked  map[string][]int32
}

// Config
func NewWorker(log *slog.Logger, name string, t *Topology, group string, brokers []string, commitInterval time.Duration) (*Worker, error) {
	tm := &TaskManager{
		tasks:    []*Task{},
		log:      log,
		topology: t,
		pgs:      t.partitionGroups(),
	}

	par := make(chan AssignedOrRevoked)

	topics := t.GetTopics()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.Balancers(NewPartitionGroupBalancer(log, t.partitionGroups())),
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

	tm.client = client

	w := &Worker{
		name:              name,
		log:               log.With("worker", name),
		group:             group,
		client:            client,
		adminClient:       kadm.NewClient(client),
		t:                 t,
		state:             StateCreated,
		assignedOrRevoked: par,
		closeRequested:    make(chan struct{}, 1),
		maxPollRecords:    10000,
		taskManager:       tm,
		commitInterval:    commitInterval,
	}
	w.closed.Add(1)
	return w, nil
}

func (r *Worker) changeState(newState RoutineState) {
	r.log.Info("Change state", "from", r.state, "to", newState)
	r.state = newState
}

func (r *Worker) Run() error {
	return r.Loop()
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

	pollCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r.cancelPoll = cancel

	r.cancelPollMtx.Unlock()

	r.log.Debug("Polling Records")
	f := r.client.PollRecords(pollCtx, r.maxPollRecords)
	r.log.Debug("Polled Records")

	if f.IsClientClosed() {
		r.changeState(StateCloseRequested)
		return
	}

	if errors.Is(f.Err(), context.Canceled) {
		return
	}

	if !errors.Is(f.Err(), context.DeadlineExceeded) {
		for _, fetchError := range f.Errors() {
			if errors.Is(fetchError.Err, context.DeadlineExceeded) {
				continue
			}
			r.log.Error("fetch error", fetchError.Err, "topic", fetchError.Topic, "partition", fetchError.Partition)
			if fetchError.Err != nil {
				r.err = fmt.Errorf("fetch error on topic %s, partition %d: %w", fetchError.Topic, fetchError.Partition, fetchError.Err)
				spew.Dump(fetchError)
				r.changeState(StateCloseRequested)
				return
			}
		}

		f.EachPartition(func(fetch kgo.FetchTopicPartition) {
			r.log.Info("Processing", "topic", fetch.Topic, "partition", fetch.Partition)
			task, err := r.taskManager.TaskFor(fetch.Topic, fetch.Partition)
			if err != nil {
				r.log.Error("failed to lookup task", err, "topic", fetch.Topic, "partition", fetch.Partition)
				r.changeState(StateCloseRequested)
				return
			}

			count := 0

			for _, record := range fetch.Records {
				count++

				recordCtx, cancel := context.WithTimeout(context.Background(), time.Second*60) // TODO make this configurable
				err := task.Process(recordCtx, record)
				cancel()
				if err != nil {
					r.log.Error("Failed to process record", err) // TODO - provide middlewares to handle this error. is it always a user code error?
					r.changeState(StateCloseRequested)
					r.err = err
					return
				}
			}
			r.log.Info("Processed", "topic", fetch.Topic, "partition", fetch.Partition)

		})

	}

	commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	if err := r.maybeCommit(commitCtx); err != nil {
		r.log.Error("failed to commit", err)
		r.changeState(StateCloseRequested)
		r.err = err
		return
	}
	r.log.Info("Committed offests")
}

func (r *Worker) maybeCommit(ctx context.Context) error {
	if time.Since(r.lastSuccessfulCommit) >= r.commitInterval {
		if err := r.client.Flush(ctx); err != nil {
			return fmt.Errorf("failed to flush: %w", err)
		}
		if err := r.taskManager.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit: %w", err)
		}
		r.lastSuccessfulCommit = time.Now()
	}

	return nil

}

func (r *Worker) handleClosed() {
	r.closed.Done()
}

func (r *Worker) handlePartitionsAssigned() {
	if err := r.taskManager.Revoked(r.newlyRevoked); err != nil {
		r.log.Error("revoked failed", err)
	}

	if err := r.taskManager.Assigned(r.newlyAssigned); err != nil {
		r.log.Error("assigned failed", err)
		r.changeState(StateCloseRequested)
		return
	}

	r.newlyAssigned = nil
	r.newlyRevoked = nil

	if len(r.taskManager.tasks) > 0 {
		r.changeState(StateRunning)
	} else {
		r.changeState(StateCreated)
	}
}

func (r *Worker) handleCloseRequested() {
	wg := sync.WaitGroup{}
	wg.Add(1)

	if err := r.client.Flush(context.TODO()); err != nil {
		r.log.Error("Failed to flush client", err)
	}
	if err := r.taskManager.Commit(context.TODO()); err != nil {
		r.log.Error("Failed to commit", err)
	}

	go func() {
		// Wait until this is closed
		for range r.assignedOrRevoked {
			// TODO: OnRevoke: do commit here if needed
		}
		wg.Done()
	}()

	err := r.taskManager.Close(context.TODO())
	if err != nil {
		r.log.Error("Failed to close tasks", err)
	}

	r.client.Close()
	close(r.assignedOrRevoked) // Can close this only after client is closed, as the client writer to that channel
	wg.Wait()
	r.changeState(StateClosed)
}

func (r *Worker) handleCreated() {
	select {
	case assignments := <-r.assignedOrRevoked:
		r.newlyAssigned = assignments.Assigned
		r.newlyRevoked = assignments.Revoked
		r.changeState(StatePartitionsAssigned)
	case <-r.closeRequested:
		r.changeState(StateCloseRequested)
	}
}

// State transitions may only be done from within the loop
func (r *Worker) Loop() error {
	for {
		switch r.state {
		case StateCreated:
			r.handleCreated()
		case StateCloseRequested:
			r.handleCloseRequested()
		case StatePartitionsAssigned:
			r.handlePartitionsAssigned()
		case StateRunning:
			r.handleRunning()
		case StateClosed:
			r.handleClosed()
			return r.err
		}
	}
}

func (r *Worker) Close() error {
	r.cancelPollMtx.Lock()
	r.closeRequested <- struct{}{}
	if r.cancelPoll != nil {
		r.cancelPoll()
	}
	r.cancelPollMtx.Unlock()
	r.closed.Wait()
	return nil
}
