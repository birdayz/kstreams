package kstreams

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/birdayz/kstreams/internal/coordination"
	"github.com/birdayz/kstreams/internal/execution"
	"github.com/birdayz/kstreams/kdag"
	"golang.org/x/sync/errgroup"
)

// ErrStateDirRequired is returned when a stateful topology is created without WithStateDir()
var ErrStateDirRequired = errors.New("kstreams: WithStateDir() is required for stateful topologies")

type App struct {
	numRoutines int
	brokers     []string
	groupName   string
	t           *kdag.DAG

	routines []*execution.Worker

	log *slog.Logger

	eg *errgroup.Group

	commitInterval time.Duration

	eosConfig EOSConfig

	// State management
	stateDir string

	// Configurable timeouts and limits
	pollTimeout           time.Duration
	recordProcessTimeout  time.Duration
	maxPollRecords        int

	// Error handling
	errorHandler execution.ErrorHandler
	dlqTopic     string
}

// New creates a new kstreams application.
// Returns an error if the configuration is invalid (e.g., stateful topology without WithStateDir()).
func New(t *kdag.DAG, groupName string, opts ...Option) (*App, error) {
	s := &App{
		numRoutines:          1,
		brokers:              []string{"localhost:9092"},
		groupName:            groupName,
		t:                    t,
		routines:             []*execution.Worker{},
		log:                  NullLogger(),
		commitInterval:       time.Second * 5,
		stateDir:             "", // Must be set via WithStateDir() for stateful topologies
		pollTimeout:          time.Second * 10,
		recordProcessTimeout: time.Second * 60,
		maxPollRecords:       10000,
	}

	for _, opt := range opts {
		opt(s)
	}

	// Validate required config for stateful topologies
	if s.stateDir == "" && t.HasStatefulProcessors() {
		return nil, ErrStateDirRequired
	}

	return s, nil
}

// MustNew creates a new kstreams application, panicking on configuration errors.
// Prefer New() for production code to handle errors gracefully.
func MustNew(t *kdag.DAG, groupName string, opts ...Option) *App {
	app, err := New(t, groupName, opts...)
	if err != nil {
		panic(err)
	}
	return app
}

// Run blocks until it's exited, either by an error or by a graceful shutdown
// triggered by a call to Close.
func (c *App) Run() error {
	grp := errgroup.Group{}
	c.eg = &grp
	for i := 0; i < c.numRoutines; i++ {
		// Convert root EOSConfig to coordination.EOSConfig
		coordEOSConfig := coordination.EOSConfig{
			Enabled:         c.eosConfig.Enabled,
			TransactionalID: c.eosConfig.TransactionalID,
		}

		routine, err := execution.NewWorker(
			c.log.WithGroup("worker"),
			fmt.Sprintf("routine-%d", i),
			c.t,
			c.groupName,
			c.brokers,
			c.commitInterval,
			coordEOSConfig,
			c.groupName, // appID = groupName
			c.stateDir,
			execution.WorkerConfig{
				PollTimeout:          c.pollTimeout,
				RecordProcessTimeout: c.recordProcessTimeout,
				MaxPollRecords:       c.maxPollRecords,
				ErrorHandler:         c.errorHandler,
				DLQTopic:             c.dlqTopic,
			})
		if err != nil {
			return err
		}
		c.routines = append(c.routines, routine)
		grp.Go(routine.Run)
	}
	return grp.Wait()
}

// Close gracefully shuts down the application
func (c *App) Close() error {
	// Signal all workers to close in parallel
	var wg sync.WaitGroup
	for _, routine := range c.routines {
		wg.Add(1)
		go func(routine *execution.Worker) {
			defer wg.Done()
			_ = routine.Close()
		}(routine)
	}
	wg.Wait()

	// Wait for errgroup if Run() was called
	if c.eg != nil {
		return c.eg.Wait()
	}
	return nil
}
