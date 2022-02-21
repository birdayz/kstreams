package streamz

import "github.com/birdayz/streamz/internal"

type Option func(*Streamz)

type Streamz struct {
	numRoutines int
	brokers     []string
	groupName   string
	t           *internal.TopologyBuilder // change to topology

	routines []*internal.StreamRoutine
}

var WithNumRoutines = func(n int) Option {
	return func(s *Streamz) {
		s.numRoutines = n
	}
}

func New(t *internal.TopologyBuilder, opts ...Option) *Streamz {
	s := &Streamz{
		numRoutines: 1,
		brokers:     []string{"localhost:9092"},
		groupName:   "streamz-app",
		t:           t,
		routines:    []*internal.StreamRoutine{},
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (c *Streamz) Start() error {
	for i := 0; i < c.numRoutines; i++ {
		routine, err := internal.NewStreamRoutine(c.t, c.groupName, c.brokers)
		if err != nil {
			return err
		}
		c.routines = append(c.routines, routine)
		routine.Start()
	}
	return nil
}

func (c *Streamz) Close() error {
	for _, routine := range c.routines {
		routine.Close()
	}
	return nil
}
