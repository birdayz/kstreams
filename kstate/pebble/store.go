package pebble

import (
	"context"
	"errors"
	"fmt"
	"iter"

	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kstate"
	"github.com/cockroachdb/pebble"
)

type pebbleStore struct {
	name string
	db   *pebble.DB
}

func (s *pebbleStore) Name() string {
	return s.name
}

func (s *pebbleStore) Init(ctx kprocessor.ProcessorContextInternal) error {
	return nil
}

func (s *pebbleStore) Persistent() bool {
	return true
}

func (s *pebbleStore) Flush(ctx context.Context) error {
	return s.db.Flush()
}

func (s *pebbleStore) Close() error {
	if err := s.db.Flush(); err != nil {
		return err
	}
	return s.db.Close()
}

func (s *pebbleStore) Set(k, v []byte) error {
	if v == nil {
		return s.db.Delete(k, &pebble.WriteOptions{})
	}
	return s.db.Set(k, v, &pebble.WriteOptions{Sync: false})
}

func (s *pebbleStore) Get(k []byte) ([]byte, error) {
	v, closer, err := s.db.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, kstate.ErrKeyNotFound
		}
		return nil, err
	}
	defer closer.Close()

	res := make([]byte, len(v))
	copy(res, v)
	return res, nil
}

func (s *pebbleStore) Delete(k []byte) error {
	return s.db.Delete(k, &pebble.WriteOptions{})
}

func (s *pebbleStore) Range(start, end []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		it := s.db.NewIter(&pebble.IterOptions{
			LowerBound: start,
			UpperBound: end,
		})
		defer it.Close()

		for it.First(); it.Valid(); it.Next() {
			key := make([]byte, len(it.Key()))
			copy(key, it.Key())

			val, err := it.ValueAndErr()
			if err != nil {
				return
			}
			value := make([]byte, len(val))
			copy(value, val)

			if !yield(key, value) {
				return
			}
		}
	}
}

func (s *pebbleStore) All() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		it := s.db.NewIter(nil)
		defer it.Close()

		for it.First(); it.Valid(); it.Next() {
			key := make([]byte, len(it.Key()))
			copy(key, it.Key())

			val, err := it.ValueAndErr()
			if err != nil {
				return
			}
			value := make([]byte, len(val))
			copy(value, val)

			if !yield(key, value) {
				return
			}
		}
	}
}

func (s *pebbleStore) ReverseRange(start, end []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		it := s.db.NewIter(&pebble.IterOptions{
			LowerBound: start,
			UpperBound: end,
		})
		defer it.Close()

		for it.Last(); it.Valid(); it.Prev() {
			key := make([]byte, len(it.Key()))
			copy(key, it.Key())

			val, err := it.ValueAndErr()
			if err != nil {
				return
			}
			value := make([]byte, len(val))
			copy(value, val)

			if !yield(key, value) {
				return
			}
		}
	}
}

func (s *pebbleStore) ReverseAll() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		it := s.db.NewIter(nil)
		defer it.Close()

		for it.Last(); it.Valid(); it.Prev() {
			key := make([]byte, len(it.Key()))
			copy(key, it.Key())

			val, err := it.ValueAndErr()
			if err != nil {
				return
			}
			value := make([]byte, len(val))
			copy(value, val)

			if !yield(key, value) {
				return
			}
		}
	}
}

func newStore(stateDir, name string, partition uint32) (kstate.StoreBackend, error) {
	if stateDir == "" {
		stateDir = "/tmp/kstreams"
	}
	dir := fmt.Sprintf("%s/%s/partition-%d", stateDir, name, partition)
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", dir, err)
	}

	return &pebbleStore{name: name, db: db}, nil
}

func NewStoreBackend(stateDir string) func(name string, p int32) (kstate.StoreBackend, error) {
	return func(name string, p int32) (kstate.StoreBackend, error) {
		return newStore(stateDir, name, uint32(p))
	}
}

var _ kstate.StoreBackend = (*pebbleStore)(nil)
