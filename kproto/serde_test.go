package kproto

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestSerializer(t *testing.T) {
	serializer := Serializer[*wrapperspb.StringValue]()

	msg := wrapperspb.String("hello world")
	data, err := serializer(msg)
	if err != nil {
		t.Fatalf("Serializer failed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Serializer returned empty data")
	}

	// Verify by deserializing
	decoded := &wrapperspb.StringValue{}
	if err := proto.Unmarshal(data, decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}
	if decoded.Value != "hello world" {
		t.Errorf("Expected 'hello world', got %q", decoded.Value)
	}
}

func TestDeserializer(t *testing.T) {
	// Serialize a message
	msg := wrapperspb.String("test value")
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	// Test Deserializer with newFn
	deserializer := Deserializer(func() *wrapperspb.StringValue {
		return &wrapperspb.StringValue{}
	})

	decoded, err := deserializer(data)
	if err != nil {
		t.Fatalf("Deserializer failed: %v", err)
	}
	if decoded.Value != "test value" {
		t.Errorf("Expected 'test value', got %q", decoded.Value)
	}
}

func TestDeserializerFor(t *testing.T) {
	// Serialize a message
	msg := wrapperspb.Int64(42)
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	// Test DeserializerFor
	deserializer := DeserializerFor[*wrapperspb.Int64Value]()

	decoded, err := deserializer(data)
	if err != nil {
		t.Fatalf("DeserializerFor failed: %v", err)
	}
	if decoded.Value != 42 {
		t.Errorf("Expected 42, got %d", decoded.Value)
	}
}

func TestRoundTrip(t *testing.T) {
	serializer := Serializer[*wrapperspb.BoolValue]()
	deserializer := DeserializerFor[*wrapperspb.BoolValue]()

	original := wrapperspb.Bool(true)
	data, err := serializer(original)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	decoded, err := deserializer(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if decoded.Value != original.Value {
		t.Errorf("Round trip failed: expected %v, got %v", original.Value, decoded.Value)
	}
}

func TestDeserializerInvalidData(t *testing.T) {
	deserializer := DeserializerFor[*wrapperspb.StringValue]()

	// Invalid protobuf data
	_, err := deserializer([]byte{0xFF, 0xFF, 0xFF})
	if err == nil {
		t.Error("Expected error for invalid data, got nil")
	}
}
