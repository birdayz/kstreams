package kproto

import (
	"context"
	"errors"
	"testing"

	"github.com/birdayz/kstreams/kprocessor"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestValidateInterceptor_ValidMessage(t *testing.T) {
	interceptor := ValidateInterceptor[string, *wrapperspb.StringValue]()

	record := kprocessor.Record[string, *wrapperspb.StringValue]{
		Key:   "test-key",
		Value: wrapperspb.String("hello"),
		Metadata: kprocessor.RecordMetadata{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    100,
		},
	}

	var handlerCalled bool
	handler := func(ctx context.Context, r kprocessor.Record[string, *wrapperspb.StringValue]) error {
		handlerCalled = true
		return nil
	}

	err := interceptor(context.Background(), record, handler)
	if err != nil {
		t.Errorf("Expected no error for valid message, got: %v", err)
	}
	if !handlerCalled {
		t.Error("Handler was not called for valid message")
	}
}

func TestValidateInterceptor_HandlerError(t *testing.T) {
	interceptor := ValidateInterceptor[string, *wrapperspb.StringValue]()

	record := kprocessor.Record[string, *wrapperspb.StringValue]{
		Key:   "test-key",
		Value: wrapperspb.String("hello"),
		Metadata: kprocessor.RecordMetadata{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    100,
		},
	}

	expectedErr := errors.New("handler error")
	handler := func(ctx context.Context, r kprocessor.Record[string, *wrapperspb.StringValue]) error {
		return expectedErr
	}

	err := interceptor(context.Background(), record, handler)
	if !errors.Is(err, expectedErr) {
		t.Errorf("Expected handler error, got: %v", err)
	}
}

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{
		Topic:     "my-topic",
		Partition: 5,
		Offset:    1234,
		Err:       errors.New("field validation failed"),
	}

	expected := "validation failed for record [topic=my-topic, partition=5, offset=1234]: field validation failed"
	if err.Error() != expected {
		t.Errorf("Error message mismatch.\nExpected: %s\nGot: %s", expected, err.Error())
	}
}

func TestValidationError_Unwrap(t *testing.T) {
	innerErr := errors.New("inner error")
	err := &ValidationError{
		Topic:     "topic",
		Partition: 0,
		Offset:    0,
		Err:       innerErr,
	}

	if !errors.Is(err, innerErr) {
		t.Error("Unwrap should return inner error")
	}
}
