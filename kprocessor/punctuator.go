package kprocessor

import (
	"context"
	"time"
)

// Punctuator is a callback function invoked on a schedule
type Punctuator func(ctx context.Context, timestamp time.Time) error

// PunctuationType defines when a punctuator should be triggered
type PunctuationType int

const (
	// PunctuateByStreamTime triggers based on the maximum timestamp seen in records
	PunctuateByStreamTime PunctuationType = iota
	// PunctuateByWallClockTime triggers based on system wall clock time
	PunctuateByWallClockTime
)

// Cancellable allows canceling a scheduled punctuation
type Cancellable interface {
	Cancel()
}
