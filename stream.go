package streamz

import (
	"fmt"

	"github.com/birdayz/streamz/internal"
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
}

var WithNumRoutines = func(n int) Option {
	return func(s *Streamz) {
		s.numRoutines = n
	}
}

var WithLogr = func(log logr.Logger) Option {
	return func(s *Streamz) {
		s.log = log
	}
}

func New(t *internal.TopologyBuilder, opts ...Option) *Streamz {
	s := &Streamz{
		numRoutines: 1,
		brokers:     []string{"localhost:9092"},
		groupName:   "streamz-app",
		t:           t,
		routines:    []*internal.Worker{},
		log:         logr.Discard(),
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
		routine, err := internal.NewWorker(c.log.WithName("worker"), fmt.Sprintf("routine-%d", i), c.t, c.groupName, c.brokers)
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

	c.eg.Wait()
	return nil
}
