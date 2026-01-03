package execution

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/birdayz/kstreams/internal/coordination"
	"github.com/birdayz/kstreams/kdag"
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
	coordinator CommitCoordinator
	adminClient *kadm.Client
	log         *slog.Logger
	group       string

	t *kdag.DAG

	state RoutineState

	assignedOrRevoked chan AssignedOrRevoked

	newlyAssigned map[string][]int32
	newlyRevoked  map[string][]int32

	closeRequested chan struct{}

	name string

	cancelPollMtx sync.Mutex
	cancelPoll    func()

	closed    sync.WaitGroup
	closeOnce sync.Once

	maxPollRecords int

	taskManager *TaskManager

	lastSuccessfulCommit time.Time
	commitInterval       time.Duration

	err error

	// State management configuration
	appID    string
	stateDir string

	// Configurable timeouts
	pollTimeout          time.Duration
	recordProcessTimeout time.Duration

	// Error handling
	errorHandler ErrorHandler
	dlqTopic     string

	// Shutdown configuration
	shutdownTimeout time.Duration
}

type AssignedOrRevoked struct {
	Assigned map[string][]int32
	Revoked  map[string][]int32
}

// DefaultShutdownTimeout is the default timeout for graceful shutdown
const DefaultShutdownTimeout = 30 * time.Second

// WorkerConfig holds configuration for a Worker
type WorkerConfig struct {
	PollTimeout          time.Duration
	RecordProcessTimeout time.Duration
	MaxPollRecords       int
	ErrorHandler         ErrorHandler
	DLQTopic             string
	ShutdownTimeout      time.Duration // Default: 30 seconds
}

// NewWorker creates a new Worker with the given configuration
func NewWorker(log *slog.Logger, name string, t *kdag.DAG, group string, brokers []string, commitInterval time.Duration, eosConfig coordination.EOSConfig, appID string, stateDir string, cfg WorkerConfig) (*Worker, error) {
	tm := &TaskManager{
		tasks:    []*Task{},
		log:      log,
		topology: t,
		pgs:      t.GetPartitionGroups(),
		appID:    appID,
		stateDir: stateDir,
	}

	par := make(chan AssignedOrRevoked, 10) // Buffered to prevent deadlock with franz-go callbacks

	topics := t.GetTopics()

	// Consumer configuration
	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.Balancers(coordination.NewPartitionGroupBalancer(log, t.GetPartitionGroups())),
		kgo.DisableAutoCommit(),
		kgo.ConsumeTopics(topics...),
		kgo.OnPartitionsAssigned(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			par <- AssignedOrRevoked{Assigned: m}
		}),
		kgo.OnPartitionsRevoked(func(c1 context.Context, c2 *kgo.Client, m map[string][]int32) {
			par <- AssignedOrRevoked{Revoked: m}
		}),
	}

	var coordinator CommitCoordinator

	// Create the appropriate coordinator based on EOS config
	if eosConfig.Enabled {
		consumerOpts = append(consumerOpts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))

		// Add transactional ID for this worker
		transactionalID := fmt.Sprintf("%s-%s", eosConfig.TransactionalID, name)
		consumerOpts = append(consumerOpts,
			kgo.TransactionalID(transactionalID),
			kgo.RequireStableFetchOffsets(), // Recommended for Kafka 2.5+ EOS
		)

		// Create GroupTransactSession for full EOS with atomic offset commits
		session, err := kgo.NewGroupTransactSession(consumerOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create group transact session: %w", err)
		}

		coordinator = NewEOSCoordinator(session, log)

		log.Info("Exactly-once semantics enabled (full EOS)",
			"transactional_id", transactionalID,
			"isolation_level", "read_committed",
			"atomic_offset_commits", true)
	} else {
		// Non-EOS: use regular client
		client, err := kgo.NewClient(consumerOpts...)
		if err != nil {
			return nil, err
		}

		coordinator = NewAtLeastOnceCoordinator(client, tm, log)
	}

	tm.client = coordinator.Client()

	// Default to fail-fast error handler if none specified
	errorHandler := cfg.ErrorHandler
	if errorHandler == nil {
		errorHandler = DefaultErrorHandler()
	}

	// Default shutdown timeout
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}

	w := &Worker{
		name:                 name,
		log:                  log.With("worker", name),
		group:                group,
		coordinator:          coordinator,
		adminClient:          kadm.NewClient(coordinator.Client()),
		t:                    t,
		state:                StateCreated,
		assignedOrRevoked:    par,
		closeRequested:       make(chan struct{}, 1),
		maxPollRecords:       cfg.MaxPollRecords,
		taskManager:          tm,
		commitInterval:       commitInterval,
		appID:                appID,
		stateDir:             stateDir,
		pollTimeout:          cfg.PollTimeout,
		recordProcessTimeout: cfg.RecordProcessTimeout,
		errorHandler:         errorHandler,
		dlqTopic:             cfg.DLQTopic,
		shutdownTimeout:      shutdownTimeout,
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

// ErrShutdownTimeout is returned when graceful shutdown exceeds the timeout
var ErrShutdownTimeout = fmt.Errorf("worker shutdown timed out")

func (r *Worker) Close() error {
	r.closeOnce.Do(func() {
		r.cancelPollMtx.Lock()
		select {
		case r.closeRequested <- struct{}{}:
			// Signal sent successfully
		default:
			// Already signaled (shouldn't happen with Once, but safe)
		}
		if r.cancelPoll != nil {
			r.cancelPoll()
		}
		r.cancelPollMtx.Unlock()
	})

	// Wait for shutdown with timeout
	done := make(chan struct{})
	go func() {
		r.closed.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(r.shutdownTimeout):
		r.log.Error("Shutdown timeout exceeded", "timeout", r.shutdownTimeout)
		return ErrShutdownTimeout
	}
}
