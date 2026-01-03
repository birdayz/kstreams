package kstreams

import (
	"log/slog"
	"time"

	"github.com/birdayz/kstreams/internal/execution"
)

// Option is a function that configures an App
type Option func(*App)

// WithWorkersCount sets the number of worker routines
var WithWorkersCount = func(n int) Option {
	return func(s *App) {
		s.numRoutines = n
	}
}

// WithLog sets the logger for the application
var WithLog = func(log *slog.Logger) Option {
	return func(s *App) {
		s.log = log
	}
}

// WithBrokers sets the Kafka broker addresses
var WithBrokers = func(brokers []string) Option {
	return func(s *App) {
		s.brokers = brokers
	}
}

// WithCommitInterval sets the commit interval
var WithCommitInterval = func(commitInterval time.Duration) Option {
	return func(s *App) {
		s.commitInterval = commitInterval
	}
}

// WithStateDir sets the state directory for state stores
var WithStateDir = func(stateDir string) Option {
	return func(s *App) {
		s.stateDir = stateDir
	}
}

// WithPollTimeout sets the timeout for polling records from Kafka
var WithPollTimeout = func(timeout time.Duration) Option {
	return func(s *App) {
		s.pollTimeout = timeout
	}
}

// WithRecordProcessTimeout sets the timeout for processing a single record
var WithRecordProcessTimeout = func(timeout time.Duration) Option {
	return func(s *App) {
		s.recordProcessTimeout = timeout
	}
}

// WithMaxPollRecords sets the maximum number of records to poll at once
var WithMaxPollRecords = func(n int) Option {
	return func(s *App) {
		s.maxPollRecords = n
	}
}

// ErrorRecovery determines how to handle a processing error
type ErrorRecovery = execution.ErrorRecovery

// Error recovery constants
const (
	RecoveryFail = execution.RecoveryFail
	RecoverySkip = execution.RecoverySkip
	RecoveryDLQ  = execution.RecoveryDLQ
)

// ErrorHandler is called when a record processing error occurs
type ErrorHandler = execution.ErrorHandler

// WithErrorHandler sets a custom error handler for processing failures.
// The handler receives the error and the failed record, and returns the recovery action.
// Default behavior is fail-fast (RecoveryFail).
var WithErrorHandler = func(handler ErrorHandler) Option {
	return func(s *App) {
		s.errorHandler = handler
	}
}

// WithDLQTopic sets the dead letter queue topic for failed records.
// Required when using RecoveryDLQ in the error handler.
var WithDLQTopic = func(topic string) Option {
	return func(s *App) {
		s.dlqTopic = topic
	}
}

// NullWriter is a writer that discards all data
type NullWriter struct{}

func (NullWriter) Write([]byte) (int, error) { return 0, nil }

// NullLogger creates a logger that discards all output
func NullLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(NullWriter{}, nil))
}
