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
