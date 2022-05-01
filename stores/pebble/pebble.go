package pebble

import (
	"context"
	"errors"
	"fmt"

	"github.com/birdayz/streamz"
	"github.com/birdayz/streamz/sdk"
	"github.com/cockroachdb/pebble"
)

type pebbleStore struct {
	db *pebble.DB
}

func (s *pebbleStore) Init() error {
	return nil
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
	return s.db.Set(k, v, &pebble.WriteOptions{Sync: false})
}

func (s *pebbleStore) Get(k []byte) ([]byte, error) {
	v, closer, err := s.db.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, streamz.ErrKeyNotFound
		}
		return nil, err
	}
	defer closer.Close()

	res := make([]byte, len(v))
	copy(res, v)

	return res, nil
}

func newStore(stateDir, name string, partition uint32) (sdk.StoreBackend, error) {
	if stateDir == "" {
		stateDir = "/tmp/streamz"
	}
	dir := fmt.Sprintf("%s/%s/partition-%d", stateDir, name, partition)
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", dir, err)
	}

	return &pebbleStore{db: db}, nil
}

func NewStoreBuilder(stateDir string) func(name string, p int32) (sdk.StoreBackend, error) {
	return func(name string, p int32) (sdk.StoreBackend, error) {
		return newStore(stateDir, name, uint32(p))
	}
}

var _ = sdk.StoreBackend(&pebbleStore{})
