package serdes

import (
	"encoding/json"

	"github.com/birdayz/kstreams/sdk"
)

func JSONSerializer[T any]() sdk.Serializer[T] {
	return func(t T) ([]byte, error) {
		serialized, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		return serialized, nil
	}
}

func JSONDeserializer[T any]() sdk.Deserializer[T] {
	return func(b []byte) (T, error) {
		var deserialized T
		if err := json.Unmarshal(b, &deserialized); err != nil {
			return *new(T), err
		}
		return deserialized, nil
	}
}

func JSON[T any]() sdk.SerDe[T] {
	return sdk.SerDe[T]{
		Serializer:   JSONSerializer[T](),
		Deserializer: JSONDeserializer[T](),
	}
}
