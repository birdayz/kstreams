package kstreams

import (
	"testing"

	"github.com/birdayz/kstreams/kdag"
)

func TestApp_CloseBeforeRun(t *testing.T) {
	// Create a minimal DAG for testing
	builder := kdag.NewBuilder()
	dag, err := builder.Build()
	if err != nil {
		t.Fatalf("failed to build DAG: %v", err)
	}

	app := MustNew(dag, "test-group")

	// Close before Run should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Close() before Run() panicked: %v", r)
		}
	}()

	err = app.Close()
	if err != nil {
		t.Errorf("Close() returned unexpected error: %v", err)
	}
}
