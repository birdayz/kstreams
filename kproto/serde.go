package kproto

import (
	"github.com/birdayz/kstreams/kserde"
	"google.golang.org/protobuf/proto"
)

// Serializer returns a protobuf serializer for any proto.Message type.
//
// Example:
//
//	serializer := kproto.Serializer[*pb.User]()
func Serializer[T proto.Message]() kserde.Serializer[T] {
	return func(v T) ([]byte, error) {
		return proto.Marshal(v)
	}
}

// Deserializer returns a protobuf deserializer for any proto.Message type.
// The newFn parameter creates a new instance of the message type.
//
// Example:
//
//	deserializer := kproto.Deserializer(func() *pb.User { return &pb.User{} })
//
// Or using the convenience function:
//
//	deserializer := kproto.DeserializerFor[*pb.User]()
func Deserializer[T proto.Message](newFn func() T) kserde.Deserializer[T] {
	return func(data []byte) (T, error) {
		msg := newFn()
		if err := proto.Unmarshal(data, msg); err != nil {
			var zero T
			return zero, err
		}
		return msg, nil
	}
}

// DeserializerFor returns a protobuf deserializer that uses reflection to create
// new instances. This is slightly less efficient than Deserializer with an explicit
// newFn, but more convenient.
//
// Example:
//
//	deserializer := kproto.DeserializerFor[*pb.User]()
func DeserializerFor[T proto.Message]() kserde.Deserializer[T] {
	return func(data []byte) (T, error) {
		var zero T
		// Create new instance via proto.Clone of zero value's ProtoReflect
		msg := zero.ProtoReflect().New().Interface().(T)
		if err := proto.Unmarshal(data, msg); err != nil {
			return zero, err
		}
		return msg, nil
	}
}
