package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kproto"
	pb "github.com/birdayz/kstreams/examples/kproto/gen"
)

func main() {
	// Demonstrate protobuf serde
	demonstrateSerde()

	// Demonstrate validation interceptor
	demonstrateValidation()
}

func demonstrateSerde() {
	fmt.Println("=== Protobuf Serde Demo ===")

	// Create serializer and deserializer
	serializer := kproto.Serializer[*pb.User]()
	deserializer := kproto.DeserializerFor[*pb.User]()

	// Create a user
	user := &pb.User{
		Id:     "user-123",
		Email:  "alice@example.com",
		Name:   "Alice",
		Age:    30,
		Status: pb.Status_STATUS_ACTIVE,
		Tags:   []string{"developer", "golang"},
	}

	// Serialize
	data, err := serializer(user)
	if err != nil {
		log.Fatalf("Failed to serialize: %v", err)
	}
	fmt.Printf("Serialized user to %d bytes\n", len(data))

	// Deserialize
	decoded, err := deserializer(data)
	if err != nil {
		log.Fatalf("Failed to deserialize: %v", err)
	}
	fmt.Printf("Deserialized user: id=%s, name=%s, email=%s\n",
		decoded.Id, decoded.Name, decoded.Email)

	fmt.Println()
}

func demonstrateValidation() {
	fmt.Println("=== Protovalidate Interceptor Demo ===")

	// Create validation interceptor for UserEvent
	validateInterceptor := kproto.ValidateInterceptor[string, *pb.UserEvent]()

	// Create an interceptor chain
	chain := kprocessor.ChainInterceptors(validateInterceptor)

	// The final handler that processes valid records
	handler := func(ctx context.Context, record kprocessor.Record[string, *pb.UserEvent]) error {
		fmt.Printf("Processing valid event: type=%s, user=%s\n",
			record.Value.EventType, record.Value.User.Name)
		return nil
	}

	// Test 1: Valid record
	fmt.Println("\n--- Test 1: Valid record ---")
	validEvent := &pb.UserEvent{
		EventType: "created",
		User: &pb.User{
			Id:     "user-456",
			Email:  "bob@example.com",
			Name:   "Bob",
			Age:    25,
			Status: pb.Status_STATUS_ACTIVE,
		},
		Timestamp: time.Now().Format(time.RFC3339),
	}

	validRecord := kprocessor.Record[string, *pb.UserEvent]{
		Key:   "user-456",
		Value: validEvent,
		Metadata: kprocessor.RecordMetadata{
			Topic:     "user-events",
			Partition: 0,
			Offset:    100,
		},
	}

	err := chain.Execute(context.Background(), validRecord, handler)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// Test 2: Invalid record (missing required field)
	fmt.Println("\n--- Test 2: Invalid record (missing user) ---")
	invalidEvent := &pb.UserEvent{
		EventType: "created",
		User:      nil, // Required field missing
		Timestamp: time.Now().Format(time.RFC3339),
	}

	invalidRecord := kprocessor.Record[string, *pb.UserEvent]{
		Key:   "user-789",
		Value: invalidEvent,
		Metadata: kprocessor.RecordMetadata{
			Topic:     "user-events",
			Partition: 0,
			Offset:    101,
		},
	}

	err = chain.Execute(context.Background(), invalidRecord, handler)
	if err != nil {
		fmt.Printf("Validation error (expected): %v\n", err)
	}

	// Test 3: Invalid record (invalid email)
	fmt.Println("\n--- Test 3: Invalid record (invalid email) ---")
	badEmailEvent := &pb.UserEvent{
		EventType: "updated",
		User: &pb.User{
			Id:     "user-999",
			Email:  "not-an-email", // Invalid email format
			Name:   "Charlie",
			Age:    35,
			Status: pb.Status_STATUS_ACTIVE,
		},
		Timestamp: time.Now().Format(time.RFC3339),
	}

	badEmailRecord := kprocessor.Record[string, *pb.UserEvent]{
		Key:   "user-999",
		Value: badEmailEvent,
		Metadata: kprocessor.RecordMetadata{
			Topic:     "user-events",
			Partition: 0,
			Offset:    102,
		},
	}

	err = chain.Execute(context.Background(), badEmailRecord, handler)
	if err != nil {
		fmt.Printf("Validation error (expected): %v\n", err)
	}

	// Test 4: Invalid record (invalid event type)
	fmt.Println("\n--- Test 4: Invalid record (invalid event type) ---")
	badTypeEvent := &pb.UserEvent{
		EventType: "invalid_type", // Not in allowed values
		User: &pb.User{
			Id:     "user-111",
			Email:  "dave@example.com",
			Name:   "Dave",
			Age:    40,
			Status: pb.Status_STATUS_ACTIVE,
		},
		Timestamp: time.Now().Format(time.RFC3339),
	}

	badTypeRecord := kprocessor.Record[string, *pb.UserEvent]{
		Key:   "user-111",
		Value: badTypeEvent,
		Metadata: kprocessor.RecordMetadata{
			Topic:     "user-events",
			Partition: 0,
			Offset:    103,
		},
	}

	err = chain.Execute(context.Background(), badTypeRecord, handler)
	if err != nil {
		fmt.Printf("Validation error (expected): %v\n", err)
	}

	fmt.Println()
}
