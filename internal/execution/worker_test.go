package execution

import (
	"sync"
	"testing"
	"time"
)

// TestWorker_DoubleClose verifies that calling Close() twice doesn't block or panic
func TestWorker_DoubleClose(t *testing.T) {
	// Create a minimal worker-like struct to test the close pattern
	w := &testCloseableWorker{
		closeRequested: make(chan struct{}, 1),
	}
	w.closed.Add(1)

	// Start a goroutine that simulates the worker loop
	go func() {
		<-w.closeRequested
		w.closed.Done()
	}()

	// First close should succeed
	err := w.Close()
	if err != nil {
		t.Fatalf("First close failed: %v", err)
	}

	// Second close should NOT block - use timeout to detect blocking
	done := make(chan struct{})
	go func() {
		_ = w.Close() // This should not block
		close(done)
	}()

	select {
	case <-done:
		// Good - didn't block
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Second Close() blocked - race condition detected")
	}
}

// TestWorker_ConcurrentClose verifies that concurrent Close() calls don't panic or deadlock
func TestWorker_ConcurrentClose(t *testing.T) {
	w := &testCloseableWorker{
		closeRequested: make(chan struct{}, 1),
	}
	w.closed.Add(1)

	// Start a goroutine that simulates the worker loop
	go func() {
		<-w.closeRequested
		w.closed.Done()
	}()

	// Launch multiple concurrent close attempts
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = w.Close()
		}()
	}

	// Use timeout to detect deadlock
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good - all closes completed
	case <-time.After(1 * time.Second):
		t.Fatal("Concurrent Close() calls deadlocked")
	}
}

// testCloseableWorker mimics Worker's close pattern for testing
type testCloseableWorker struct {
	closeRequested chan struct{}
	closed         sync.WaitGroup
	cancelPollMtx  sync.Mutex
	closeOnce      sync.Once
}

func (w *testCloseableWorker) Close() error {
	w.closeOnce.Do(func() {
		w.cancelPollMtx.Lock()
		select {
		case w.closeRequested <- struct{}{}:
			// Signal sent
		default:
			// Already signaled
		}
		w.cancelPollMtx.Unlock()
	})
	w.closed.Wait()
	return nil
}
