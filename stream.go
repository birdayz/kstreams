package kstreams

import (
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

type Option func(*App)

type App struct {
	numRoutines int
	brokers     []string
	groupName   string
	t           *Topology

	routines []*Worker

	log *slog.Logger

	eg *errgroup.Group

	commitInterval time.Duration
}

var WithWorkersCount = func(n int) Option {
	return func(s *App) {
		s.numRoutines = n
	}
}

var WithLog = func(log *slog.Logger) Option {
	return func(s *App) {
		s.log = log
	}
}

var WithBrokers = func(brokers []string) Option {
	return func(s *App) {
		s.brokers = brokers
	}
}

var WithCommitInterval = func(commitInterval time.Duration) Option {
	return func(s *App) {
		s.commitInterval = commitInterval
	}
}

type NullWriter struct{}

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

func NullLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(NullWriter{}, nil))
}

func New(t *Topology, groupName string, opts ...Option) *App {
	s := &App{
		numRoutines:    1,
		brokers:        []string{"localhost:9092"},
		groupName:      groupName,
		t:              t,
		routines:       []*Worker{},
		log:            NullLogger(),
		commitInterval: time.Second * 5,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Run blocks until it's exited, either by an error or by a graceful shutdown
// triggered by a call to Close.
func (c *App) Run() error {
	grp := errgroup.Group{}
	c.eg = &grp
	for i := 0; i < c.numRoutines; i++ {
		routine, err := NewWorker(
			c.log.WithGroup("worker"),
			fmt.Sprintf("routine-%d", i),
			c.t,
			c.groupName,
			c.brokers,
			c.commitInterval)
		if err != nil {
			return err
		}
		c.routines = append(c.routines, routine)
		grp.Go(routine.Run)
	}
	return grp.Wait()
}

func (c *App) Close() error {
	for _, routine := range c.routines {
		go func(routine *Worker) {
			routine.Close()
		}(routine)
	}

	return c.eg.Wait()
}
