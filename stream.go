package kstreams

import (
	"fmt"
	"time"

	"github.com/birdayz/kstreams/internal"
	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
)

type Option func(*Streamz)

type Streamz struct {
	numRoutines int
	brokers     []string
	groupName   string
	t           *internal.TopologyBuilder // change to topology

	routines []*internal.Worker

	log logr.Logger

	eg *errgroup.Group

	commitInterval time.Duration
}

var WithWorkersCount = func(n int) Option {
	return func(s *Streamz) {
		s.numRoutines = n
	}
}

var WithLogr = func(log logr.Logger) Option {
	return func(s *Streamz) {
		s.log = log
	}
}

var WithBrokers = func(brokers []string) Option {
	return func(s *Streamz) {
		s.brokers = brokers
	}
}

var WithCommitInterval = func(commitInterval time.Duration) Option {
	return func(s *Streamz) {
		s.commitInterval = commitInterval
	}
}

func New(t *internal.TopologyBuilder, groupName string, opts ...Option) *Streamz {
	s := &Streamz{
		numRoutines:    1,
		brokers:        []string{"localhost:9092"},
		groupName:      groupName,
		t:              t,
		routines:       []*internal.Worker{},
		log:            logr.Discard(),
		commitInterval: time.Second * 10,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Run blocks until it's exited, either by an error or by a graceful shutdown
// triggered by a call to Close.
func (c *Streamz) Run() error {
	grp := errgroup.Group{}
	c.eg = &grp
	for i := 0; i < c.numRoutines; i++ {
		routine, err := internal.NewWorker(c.log.WithName("worker"), fmt.Sprintf("routine-%d", i), c.t, c.groupName, c.brokers, c.commitInterval)
		if err != nil {
			return err
		}
		c.routines = append(c.routines, routine)
		grp.Go(routine.Run)
	}
	return grp.Wait()
}

func (c *Streamz) Close() error {
	for _, routine := range c.routines {
		go func(routine *internal.Worker) {
			routine.Close()
		}(routine)
	}

	return c.eg.Wait()
}
