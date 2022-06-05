package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/birdayz/kstreams/sdk"
)

func RegisterStore(t *TopologyBuilder, storeBuilder sdk.StoreBuilder, name string) {
	t.stores[name] = storeBuilder
}

func NewKeyValueStore[K, V any](
	store sdk.StoreBackend,
	keySerializer sdk.Serializer[K],
	valueSerializer sdk.Serializer[V],
	keyDeserializer sdk.Deserializer[K],
	valueDeserializer sdk.Deserializer[V],
) *KeyValueStore[K, V] {
	return &KeyValueStore[K, V]{
		store:             store,
		keySerializer:     keySerializer,
		valueSerializer:   valueSerializer,
		keyDeserializer:   keyDeserializer,
		valueDeserializer: valueDeserializer,
	}
}

type KeyValueStore[K, V any] struct {
	store             sdk.StoreBackend
	keySerializer     sdk.Serializer[K]
	valueSerializer   sdk.Serializer[V]
	keyDeserializer   sdk.Deserializer[K]
	valueDeserializer sdk.Deserializer[V]
}

func (t *KeyValueStore[K, V]) Init() error {
	return t.store.Init()
}

func (t *KeyValueStore[K, V]) Flush(ctx context.Context) error {
	return t.store.Flush(ctx)
}

func (t *KeyValueStore[K, V]) Close() error {
	return t.store.Close()
}

func (t *KeyValueStore[K, V]) Set(k K, v V) error {
	key, err := t.keySerializer(k)
	if err != nil {
		return err
	}

	value, err := t.valueSerializer(v)
	if err != nil {
		return err
	}

	return t.store.Set(key, value)
}

func (t *KeyValueStore[K, V]) Get(k K) (V, error) {
	var v V
	key, err := t.keySerializer(k)
	if err != nil {
		return v, err
	}

	res, err := t.store.Get(key)
	if err != nil {
		return v, err
	}

	return t.valueDeserializer(res)
}

type WindowedKeyValueStore[K, V any] struct {
	store sdk.KeyValueStore[K, V]
}

// type for windowed key

type WindowKey[K any] struct {
	Key  K
	Time time.Time
}

func WindowKeySerializer[K any](serializer sdk.Serializer[K]) sdk.Serializer[WindowKey[K]] {
	return func(wk WindowKey[K]) ([]byte, error) {
		buf := bytes.NewBuffer(nil)

		// It might be interesting, if serializers are not just functions but
		// interfaces, and can optionally implement "MarshalTo", which directly
		// writes to an io.Writer.
		serializedKey, err := serializer(wk.Key)
		if err != nil {
			return nil, err
		}

		lnPrefix := make([]byte, 2)
		binary.BigEndian.PutUint16(lnPrefix, uint16(len(serializedKey))) // TODO careful
		if _, err := buf.Write(lnPrefix); err != nil {
			return nil, err
		}

		fmt.Println(buf.Len())

		if _, err := buf.Write(serializedKey); err != nil {
			return nil, err
		}

		fmt.Println(buf.Len())

		ts, err := wk.Time.MarshalBinary()
		if _, err := buf.Write(ts); err != nil {
			return nil, err
		}

		fmt.Println(buf.Len())

		fmt.Println("x", buf.Bytes()[0])
		fmt.Println("y", buf.Bytes()[1])

		return buf.Bytes(), nil
	}
}

func WindowKeyDeserializer[K any](deserializer sdk.Deserializer[K]) sdk.Deserializer[WindowKey[K]] {
	return func(b []byte) (key WindowKey[K], err error) {
		fmt.Println("d", b[0])
		fmt.Println("x", b[1])
		length := binary.BigEndian.Uint16(b)
		fmt.Println(len(b), int(length)+1+8)
		if len(b) < int(length)+1+8 {
			return WindowKey[K]{}, fmt.Errorf("eof")
		}

		b = b[2:]

		deserialized, err := deserializer(b[:length])
		if err != nil {
			return WindowKey[K]{}, err
		}

		b = b[length:]

		var t time.Time
		err = t.UnmarshalBinary(b)
		if err != nil {
			return WindowKey[K]{}, err
		}

		return WindowKey[K]{
			Key:  deserialized,
			Time: t,
		}, nil
	}
}

func (s *WindowedKeyValueStore[K, V]) Set(k K, v V, t time.Time) {
}
