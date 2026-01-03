package execution

import (
	"context"
	"fmt"

	"github.com/birdayz/kstreams/internal/runtime"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchRawRecordProcessor processes batches of raw Kafka records
type BatchRawRecordProcessor interface {
	runtime.RawRecordProcessor
	ProcessBatch(ctx context.Context, records []*kgo.Record) error
}

// ErrorRecovery determines how to handle a processing error
type ErrorRecovery int

const (
	// RecoveryFail closes the worker (default behavior)
	RecoveryFail ErrorRecovery = iota
	// RecoverySkip skips the record and continues processing
	RecoverySkip
	// RecoveryDLQ sends the record to a dead letter queue and continues
	RecoveryDLQ
)

// ErrorHandler is called when a record processing error occurs.
// It receives the context, error, and the record that failed.
// Returns the desired recovery action.
type ErrorHandler func(ctx context.Context, err error, record *kgo.Record) ErrorRecovery

// ErrorHandlerConfig configures error handling behavior
type ErrorHandlerConfig struct {
	Handler  ErrorHandler
	DLQTopic string // Required if RecoveryDLQ is used
}

// DefaultErrorHandler returns RecoveryFail for all errors (fail-fast behavior)
func DefaultErrorHandler() ErrorHandler {
	return func(ctx context.Context, err error, record *kgo.Record) ErrorRecovery {
		return RecoveryFail
	}
}

// ProcessingStage indicates where in the pipeline an error occurred
type ProcessingStage string

const (
	StageDeserialization ProcessingStage = "deserialization"
	StageProcessing      ProcessingStage = "processing"
	StageSerialization   ProcessingStage = "serialization"
	StageStateStore      ProcessingStage = "state_store"
	StageForward         ProcessingStage = "forward"
	StageCommit          ProcessingStage = "commit"
)

// ProcessingError wraps an error with source attribution for debugging.
// It identifies which processor failed and at what stage.
type ProcessingError struct {
	// Cause is the underlying error
	Cause error

	// Stage identifies where in the pipeline the error occurred
	Stage ProcessingStage

	// ProcessorName is the name of the processor that failed
	ProcessorName string

	// Topic is the source topic of the record being processed
	Topic string

	// Partition is the partition of the record being processed
	Partition int32

	// Offset is the offset of the record being processed
	Offset int64
}

func (e *ProcessingError) Error() string {
	return fmt.Sprintf("%s error in processor %q (topic=%s partition=%d offset=%d): %v",
		e.Stage, e.ProcessorName, e.Topic, e.Partition, e.Offset, e.Cause)
}

func (e *ProcessingError) Unwrap() error {
	return e.Cause
}

// NewProcessingError creates a ProcessingError with full attribution
func NewProcessingError(cause error, stage ProcessingStage, processorName string, record *kgo.Record) *ProcessingError {
	return &ProcessingError{
		Cause:         cause,
		Stage:         stage,
		ProcessorName: processorName,
		Topic:         record.Topic,
		Partition:     record.Partition,
		Offset:        record.Offset,
	}
}
