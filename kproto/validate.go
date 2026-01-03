package kproto

import (
	"context"
	"fmt"

	"github.com/birdayz/kstreams/kprocessor"
	"github.com/bufbuild/protovalidate-go"
	"google.golang.org/protobuf/proto"
)

// ValidationError wraps a protovalidate error with record context
type ValidationError struct {
	Topic     string
	Partition int32
	Offset    int64
	Err       error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation failed for record [topic=%s, partition=%d, offset=%d]: %v",
		e.Topic, e.Partition, e.Offset, e.Err)
}

func (e *ValidationError) Unwrap() error {
	return e.Err
}

// ValidateInterceptor creates an interceptor that validates protobuf messages
// using protovalidate. It validates the Value of each record.
//
// Example:
//
//	interceptor := kproto.ValidateInterceptor[string, *pb.User]()
//	chain := kprocessor.ChainInterceptors(interceptor)
func ValidateInterceptor[Kin any, Vin proto.Message]() kprocessor.ProcessorInterceptor[Kin, Vin] {
	validator, err := protovalidate.New()
	if err != nil {
		panic(fmt.Sprintf("failed to create protovalidate validator: %v", err))
	}

	return func(ctx context.Context, record kprocessor.Record[Kin, Vin], handler kprocessor.ProcessorHandler[Kin, Vin]) error {
		if err := validator.Validate(record.Value); err != nil {
			return &ValidationError{
				Topic:     record.Metadata.Topic,
				Partition: record.Metadata.Partition,
				Offset:    record.Metadata.Offset,
				Err:       err,
			}
		}
		return handler(ctx, record)
	}
}

// ValidateKeyValueInterceptor creates an interceptor that validates both
// key and value protobuf messages using protovalidate.
//
// Example:
//
//	interceptor := kproto.ValidateKeyValueInterceptor[*pb.UserKey, *pb.User]()
//	chain := kprocessor.ChainInterceptors(interceptor)
func ValidateKeyValueInterceptor[Kin proto.Message, Vin proto.Message]() kprocessor.ProcessorInterceptor[Kin, Vin] {
	validator, err := protovalidate.New()
	if err != nil {
		panic(fmt.Sprintf("failed to create protovalidate validator: %v", err))
	}

	return func(ctx context.Context, record kprocessor.Record[Kin, Vin], handler kprocessor.ProcessorHandler[Kin, Vin]) error {
		if err := validator.Validate(record.Key); err != nil {
			return &ValidationError{
				Topic:     record.Metadata.Topic,
				Partition: record.Metadata.Partition,
				Offset:    record.Metadata.Offset,
				Err:       fmt.Errorf("key validation: %w", err),
			}
		}
		if err := validator.Validate(record.Value); err != nil {
			return &ValidationError{
				Topic:     record.Metadata.Topic,
				Partition: record.Metadata.Partition,
				Offset:    record.Metadata.Offset,
				Err:       fmt.Errorf("value validation: %w", err),
			}
		}
		return handler(ctx, record)
	}
}

// ValidateInterceptorWithValidator creates an interceptor using a pre-configured
// protovalidate.Validator. Use this when you need custom validator options.
//
// Example:
//
//	validator, _ := protovalidate.New(protovalidate.WithFailFast(true))
//	interceptor := kproto.ValidateInterceptorWithValidator[string, *pb.User](validator)
func ValidateInterceptorWithValidator[Kin any, Vin proto.Message](validator *protovalidate.Validator) kprocessor.ProcessorInterceptor[Kin, Vin] {
	return func(ctx context.Context, record kprocessor.Record[Kin, Vin], handler kprocessor.ProcessorHandler[Kin, Vin]) error {
		if err := validator.Validate(record.Value); err != nil {
			return &ValidationError{
				Topic:     record.Metadata.Topic,
				Partition: record.Metadata.Partition,
				Offset:    record.Metadata.Offset,
				Err:       err,
			}
		}
		return handler(ctx, record)
	}
}
