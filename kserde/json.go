package kserde

import (
	"encoding/json"

)

func JSONSerializer[T any]() Serializer[T] {
	return func(t T) ([]byte, error) {
		serialized, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		return serialized, nil
	}
}

func JSONDeserializer[T any]() Deserializer[T] {
	return func(b []byte) (T, error) {
		var deserialized T
		if err := json.Unmarshal(b, &deserialized); err != nil {
			return *new(T), err
		}
		return deserialized, nil
	}
}

func JSON[T any]() Serde[T] {
	return Serde[T]{
		Serializer:   JSONSerializer[T](),
		Deserializer: JSONDeserializer[T](),
	}
}
