package serde

import (
	"encoding/json"

	"github.com/birdayz/kstreams"
)

func JSONSerializer[T any]() kstreams.Serializer[T] {
	return func(t T) ([]byte, error) {
		serialized, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		return serialized, nil
	}
}

func JSONDeserializer[T any]() kstreams.Deserializer[T] {
	return func(b []byte) (T, error) {
		var deserialized T
		if err := json.Unmarshal(b, &deserialized); err != nil {
			return *new(T), err
		}
		return deserialized, nil
	}
}

func JSON[T any]() kstreams.SerDe[T] {
	return kstreams.SerDe[T]{
		Serializer:   JSONSerializer[T](),
		Deserializer: JSONDeserializer[T](),
	}
}
