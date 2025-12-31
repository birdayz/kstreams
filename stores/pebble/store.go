package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/birdayz/kstreams"
	"github.com/cockroachdb/pebble"
)

type pebbleStore struct {
	db *pebble.DB
}

func (s *pebbleStore) Init() error {
	return nil
}

func (s *pebbleStore) Flush() error {
	return s.db.Flush()
}

func (s *pebbleStore) Checkpoint(ctx context.Context, id string) error {
	return nil
}

func (s *pebbleStore) Close() error {
	if err := s.db.Flush(); err != nil {
		return err
	}
	return s.db.Close()
}

func (s *pebbleStore) Set(k, v []byte) error {
	// Treat nil (==tombstone) as delete
	if v == nil {
		return s.db.Delete(k, &pebble.WriteOptions{})
	}
	return s.db.Set(k, v, &pebble.WriteOptions{Sync: false})
}

func (s *pebbleStore) Get(k []byte) ([]byte, error) {
	v, closer, err := s.db.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, kstreams.ErrKeyNotFound
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

func (s *pebbleStore) Range(start, end []byte) (kstreams.Iterator, error) {
	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})

	return &pebbleIterator{iter: iter}, nil
}

func (s *pebbleStore) All() (kstreams.Iterator, error) {
	iter := s.db.NewIter(nil)

	return &pebbleIterator{iter: iter}, nil
}

func (s *pebbleStore) ReverseRange(start, end []byte) (kstreams.Iterator, error) {
	iter := s.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})

	return &pebbleIterator{iter: iter, reverse: true}, nil
}

func (s *pebbleStore) ReverseAll() (kstreams.Iterator, error) {
	iter := s.db.NewIter(nil)

	return &pebbleIterator{iter: iter, reverse: true}, nil
}

// pebbleIterator wraps pebble.Iterator to implement kstreams.Iterator
type pebbleIterator struct {
	iter    *pebble.Iterator
	reverse bool
	started bool
	valid   bool
	err     error
}

func (it *pebbleIterator) Next() bool {
	if it.err != nil {
		return false
	}

	if !it.started {
		it.started = true
		if it.reverse {
			it.valid = it.iter.Last()
		} else {
			it.valid = it.iter.First()
		}
	} else {
		if it.reverse {
			it.valid = it.iter.Prev()
		} else {
			it.valid = it.iter.Next()
		}
	}

	if !it.valid {
		it.err = it.iter.Error()
		return false
	}

	return true
}

func (it *pebbleIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	// Make a copy to avoid data races
	key := it.iter.Key()
	result := make([]byte, len(key))
	copy(result, key)
	return result
}

func (it *pebbleIterator) Value() []byte {
	if !it.valid {
		return nil
	}
	// Make a copy to avoid data races
	value, err := it.iter.ValueAndErr()
	if err != nil {
		it.err = err
		return nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result
}

func (it *pebbleIterator) Err() error {
	return it.err
}

func (it *pebbleIterator) Close() error {
	return it.iter.Close()
}

func newStore(stateDir, name string, partition uint32) (kstreams.StoreBackend, error) {
	if stateDir == "" {
		stateDir = "/tmp/kstreams"
	}
	dir := fmt.Sprintf("%s/%s/partition-%d", stateDir, name, partition)
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", dir, err)
	}

	return &pebbleStore{db: db}, nil
}

func NewStoreBackend(stateDir string) func(name string, p int32) (kstreams.StoreBackend, error) {
	return func(name string, p int32) (kstreams.StoreBackend, error) {
		return newStore(stateDir, name, uint32(p))
	}
}

var _ = kstreams.StoreBackend(&pebbleStore{})
