package kstreams

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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

	eosConfig EOSConfig
	// For full EOS with atomic offset commits
	session *kgo.GroupTransactSession

	// State management configuration
	appID    string
	stateDir string
}

type AssignedOrRevoked struct {
	Assigned map[string][]int32
	Revoked  map[string][]int32
}

// Config
func NewWorker(log *slog.Logger, name string, t *Topology, group string, brokers []string, commitInterval time.Duration, eosConfig EOSConfig, appID string, stateDir string) (*Worker, error) {
	tm := &TaskManager{
		tasks:    []*Task{},
		log:      log,
		topology: t,
		pgs:      t.getPartitionGroups(),
		appID:    appID,
		stateDir: stateDir,
	}

	par := make(chan AssignedOrRevoked)

	topics := t.GetTopics()

	// Consumer configuration
	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.Balancers(NewPartitionGroupBalancer(log, t.getPartitionGroups())),
		kgo.DisableAutoCommit(),
		kgo.ConsumeTopics(topics...),
		kgo.OnPartitionsAssigned(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			par <- AssignedOrRevoked{Assigned: m}
		}),
		kgo.OnPartitionsRevoked(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			par <- AssignedOrRevoked{Revoked: m}
		}),
	}

	var client *kgo.Client
	var session *kgo.GroupTransactSession

	// If EOS is enabled, use GroupTransactSession for atomic offset commits
	if eosConfig.Enabled {
		consumerOpts = append(consumerOpts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))

		// Add transactional ID for this worker
		transactionalID := fmt.Sprintf("%s-%s", eosConfig.TransactionalID, name)
		consumerOpts = append(consumerOpts,
			kgo.TransactionalID(transactionalID),
			kgo.RequireStableFetchOffsets(), // Recommended for Kafka 2.5+ EOS
		)

		// Create GroupTransactSession for full EOS with atomic offset commits
		var err error
		session, err = kgo.NewGroupTransactSession(consumerOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create group transact session: %w", err)
		}

		// Get underlying client for admin operations and task manager
		client = session.Client()

		log.Info("Exactly-once semantics enabled (full EOS)",
			"transactional_id", transactionalID,
			"isolation_level", "read_committed",
			"atomic_offset_commits", true)
	} else {
		// Non-EOS: use regular client
		var err error
		client, err = kgo.NewClient(consumerOpts...)
		if err != nil {
			return nil, err
		}
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
		eosConfig:         eosConfig,
		session:           session, // nil for non-EOS
		appID:             appID,
		stateDir:          stateDir,
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

	// Begin transaction if EOS is enabled (using GroupTransactSession)
	if r.eosConfig.Enabled {
		if err := r.session.Begin(); err != nil {
			r.log.Error("failed to begin transaction", "error", err)
			r.changeState(StateCloseRequested)
			r.err = err
			r.cancelPollMtx.Unlock()
			return
		}
	}

	pollCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	r.cancelPoll = cancel

	r.cancelPollMtx.Unlock()

	// Track if we need to abort transaction on error
	processingError := false

	r.log.Debug("Polling Records")
	// Use session.PollRecords() for EOS, client.PollRecords() for non-EOS
	var f kgo.Fetches
	if r.eosConfig.Enabled {
		f = r.session.PollRecords(pollCtx, r.maxPollRecords)
	} else {
		f = r.client.PollRecords(pollCtx, r.maxPollRecords)
	}
	r.log.Debug("Polled Records")

	if f.IsClientClosed() {
		r.changeState(StateCloseRequested)
		if r.eosConfig.Enabled {
			// Session will abort automatically on close
			_, _ = r.session.End(context.Background(), kgo.TryAbort)
		}
		return
	}

	if errors.Is(f.Err(), context.Canceled) {
		if r.eosConfig.Enabled {
			_, _ = r.session.End(context.Background(), kgo.TryAbort)
		}
		return
	}

	if !errors.Is(f.Err(), context.DeadlineExceeded) {
		for _, fetchError := range f.Errors() {
			if errors.Is(fetchError.Err, context.DeadlineExceeded) {
				continue
			}
			r.log.Error("fetch error", "error", fetchError.Err, "topic", fetchError.Topic, "partition", fetchError.Partition)
			if fetchError.Err != nil {
				r.err = fmt.Errorf("fetch error on topic %s, partition %d: %w", fetchError.Topic, fetchError.Partition, fetchError.Err)
				r.changeState(StateCloseRequested)
				if r.eosConfig.Enabled {
					_, _ = r.session.End(context.Background(), kgo.TryAbort)
				}
				return
			}
		}

		f.EachPartition(func(fetch kgo.FetchTopicPartition) {
			if processingError {
				return // Skip if we already had an error
			}

			r.log.Info("Processing", "topic", fetch.Topic, "partition", fetch.Partition, "len", len(fetch.Records))
			task, err := r.taskManager.TaskFor(fetch.Topic, fetch.Partition)
			if err != nil {
				r.log.Error("failed to lookup task", "error", err, "topic", fetch.Topic, "partition", fetch.Partition)
				r.changeState(StateCloseRequested)
				processingError = true
				return
			}

			for _, record := range fetch.Records {
				recordCtx, cancel := context.WithTimeout(context.Background(), time.Second*60) // TODO make this configurable
				err := task.Process(recordCtx, record)
				cancel()
				if err != nil {
					r.log.Error("Failed to process record", "error", err) // TODO - provide middlewares to handle this error. is it always a user code error?
					r.changeState(StateCloseRequested)
					r.err = err
					processingError = true
					return
				}
			}
			r.log.Info("Processed", "topic", fetch.Topic, "partition", fetch.Partition)

		})

	}

	// If there was a processing error, abort transaction and return
	if processingError {
		if r.eosConfig.Enabled {
			endCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			committed, err := r.session.End(endCtx, kgo.TryAbort)
			if err != nil {
				r.log.Error("failed to end transaction (abort)", "error", err)
			} else if !committed {
				r.log.Info("Transaction aborted due to processing error")
			}
		}
		return
	}

	commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Commit with or without transaction depending on EOS config
	if r.eosConfig.Enabled {
		// Use session.End() for atomic transaction + offset commit
		if err := r.commitWithTransactionSession(commitCtx); err != nil {
			r.log.Error("failed to commit transaction", "error", err)
			r.changeState(StateCloseRequested)
			r.err = err
			return
		}
		r.log.Info("Transaction and offsets committed atomically")
	} else {
		if err := r.maybeCommit(commitCtx); err != nil {
			r.log.Error("failed to commit", "error", err)
			r.changeState(StateCloseRequested)
			r.err = err
			return
		}
		r.log.Info("Committed offsets")
	}
}

// commitWithTransactionSession commits transaction and offsets atomically using GroupTransactSession
// This achieves true exactly-once semantics with atomic offset commits
func (r *Worker) commitWithTransactionSession(ctx context.Context) error {
	// Flush tasks (state stores) before committing transaction
	for _, task := range r.taskManager.tasks {
		if err := task.Flush(ctx); err != nil {
			// If flush fails, abort the transaction
			_, _ = r.session.End(ctx, kgo.TryAbort)
			return fmt.Errorf("failed to flush task: %w", err)
		}
	}

	// session.End() does three things atomically:
	// 1. Flushes the producer (ensures all records are sent)
	// 2. Commits the transaction (makes produced records visible)
	// 3. Commits consumer offsets within the transaction
	//
	// If the group rebalanced since Begin(), this will abort instead of commit
	committed, err := r.session.End(ctx, kgo.TryCommit)
	if err != nil {
		return fmt.Errorf("failed to end transaction: %w", err)
	}

	if !committed {
		// Transaction was aborted (likely due to rebalance)
		return fmt.Errorf("transaction aborted (likely due to rebalance)")
	}

	// CRITICAL: Checkpoint after successful EOS commit
	// This is essential for EOS - without it:
	// 1. Checkpoint is never written during processing
	// 2. On restart, full restoration from beginning (massive delay)
	// 3. Violates Kafka Streams' EOS checkpoint semantics
	//
	// Matches Kafka Streams' StreamTask.postCommit() in EOS mode
	for _, task := range r.taskManager.tasks {
		if err := task.Checkpoint(ctx); err != nil {
			r.log.Error("Failed to checkpoint task after EOS commit",
				"task", task,
				"error", err)
			// Log but don't fail - transaction is already committed
			// This matches Kafka Streams behavior
		} else {
			r.log.Debug("Checkpointed task after EOS commit", "task", task)
		}
	}

	r.lastSuccessfulCommit = time.Now()
	return nil
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
		r.log.Error("revoked failed", "error", err)
	}

	if err := r.taskManager.Assigned(r.newlyAssigned); err != nil {
		r.log.Error("assigned failed", "error", err)
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

	// For EOS, session manages flush and commit
	// For non-EOS, manually flush and commit
	if !r.eosConfig.Enabled {
		if err := r.client.Flush(context.TODO()); err != nil {
			r.log.Error("Failed to flush client", "error", err)
		}
		if err := r.taskManager.Commit(context.TODO()); err != nil {
			r.log.Error("Failed to commit", "error", err)
		}
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
		r.log.Error("Failed to close tasks", "error", err)
	}

	// Close session (for EOS) or client (for non-EOS)
	if r.eosConfig.Enabled && r.session != nil {
		r.session.Close()
	} else {
		r.client.Close()
	}

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
