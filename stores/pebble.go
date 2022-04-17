package stores

import (
	"errors"
	"fmt"

	"github.com/birdayz/streamz/sdk"
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
			return nil, sdk.ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()

	res := make([]byte, len(v))
	copy(res, v)

	return res, nil
}

func NewPersistent(stateDir, name string, partition uint32) (sdk.KeyValueByteStore, error) {
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

var _ = sdk.KeyValueByteStore(&pebbleStore{})
