package execution

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func (r *Worker) handleCreated() {
	select {
	case <-r.partitionEventNotify:
		// Check atomic for the actual event data
		if event := r.pendingPartitionEvent.Swap(nil); event != nil {
			r.newlyAssigned = event.Assigned
			r.newlyRevoked = event.Revoked
			r.changeState(StatePartitionsAssigned)
		}
		// If event was nil (consumed elsewhere), just loop again
	case <-r.closeRequested:
		r.changeState(StateCloseRequested)
	}
}

func (r *Worker) handlePartitionsAssigned() {
	if err := r.taskManager.Revoked(r.newlyRevoked); err != nil {
		r.log.Error("revoked failed", "error", err)
		r.err = err
		r.changeState(StateCloseRequested)
		return
	}

	if err := r.taskManager.Assigned(r.newlyAssigned); err != nil {
		r.log.Error("assigned failed", "error", err)
		r.err = err
		r.changeState(StateCloseRequested)
		return
	}

	r.newlyAssigned = nil
	r.newlyRevoked = nil

	if len(r.taskManager.tasks) > 0 {
		r.changeState(StateRunning)
	} else {
		r.changeState(StateCreated)
	}
}

func (r *Worker) handleRunning() {
	r.cancelPollMtx.Lock()

	// Check for pending partition event (non-blocking via atomic)
	if event := r.pendingPartitionEvent.Swap(nil); event != nil {
		r.newlyAssigned = event.Assigned
		r.newlyRevoked = event.Revoked
		r.changeState(StatePartitionsAssigned)
		r.cancelPollMtx.Unlock()
		return
	}

	select {
	case <-r.closeRequested:
		r.changeState(StateCloseRequested)
		r.cancelPollMtx.Unlock()
		return
	default:
	}

	// Begin transaction/commit cycle
	if err := r.coordinator.Begin(context.Background()); err != nil {
		r.log.Error("failed to begin transaction", "error", err)
		r.changeState(StateCloseRequested)
		r.err = err
		r.cancelPollMtx.Unlock()
		return
	}

	pollCtx, cancel := context.WithTimeout(context.Background(), r.pollTimeout)
	defer cancel()
	r.cancelPoll = cancel

	r.cancelPollMtx.Unlock()

	// Track if we need to abort transaction on error
	processingError := false

	r.log.Debug("Polling Records")
	f := r.coordinator.PollRecords(pollCtx, r.maxPollRecords)
	r.log.Debug("Polled Records")

	if f.IsClientClosed() {
		r.changeState(StateCloseRequested)
		_ = r.coordinator.Abort(context.Background())
		return
	}

	if errors.Is(f.Err(), context.Canceled) {
		_ = r.coordinator.Abort(context.Background())
		return
	}

	if !errors.Is(f.Err(), context.DeadlineExceeded) {
		for _, fetchError := range f.Errors() {
			if errors.Is(fetchError.Err, context.DeadlineExceeded) {
				continue
			}
			r.log.Error("fetch error", "error", fetchError.Err, "topic", fetchError.Topic, "partition", fetchError.Partition)
			if fetchError.Err != nil {
				r.err = fmt.Errorf("fetch error on topic %s, partition %d: %w", fetchError.Topic, fetchError.Partition, fetchError.Err)
				r.changeState(StateCloseRequested)
				_ = r.coordinator.Abort(context.Background())
				return
			}
		}

		f.EachPartition(func(fetch kgo.FetchTopicPartition) {
			if processingError {
				return // Skip if we already had an error
			}

			r.log.Info("Processing", "topic", fetch.Topic, "partition", fetch.Partition, "len", len(fetch.Records))
			task, err := r.taskManager.TaskFor(fetch.Topic, fetch.Partition)
			if err != nil {
				r.log.Error("failed to lookup task", "error", err, "topic", fetch.Topic, "partition", fetch.Partition)
				r.changeState(StateCloseRequested)
				processingError = true
				return
			}

			for _, record := range fetch.Records {
				recordCtx, cancel := context.WithTimeout(context.Background(), r.recordProcessTimeout)
				err := task.Process(recordCtx, record)
				cancel()
				if err != nil {
					recovery := r.errorHandler(recordCtx, err, record)
					switch recovery {
					case RecoveryFail:
						r.log.Error("Failed to process record, closing worker", "error", err,
							"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
						r.changeState(StateCloseRequested)
						r.err = err
						processingError = true
						return
					case RecoverySkip:
						r.log.Warn("Skipping failed record", "error", err,
							"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
						continue
					case RecoveryDLQ:
						if r.dlqTopic == "" {
							r.log.Error("DLQ recovery requested but no DLQ topic configured", "error", err)
							r.changeState(StateCloseRequested)
							r.err = fmt.Errorf("DLQ recovery requested but no DLQ topic configured: %w", err)
							processingError = true
							return
						}
						if sendErr := r.sendToDLQ(recordCtx, record, err); sendErr != nil {
							r.log.Error("Failed to send record to DLQ", "error", sendErr,
								"topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
							r.changeState(StateCloseRequested)
							r.err = fmt.Errorf("failed to send to DLQ: %w", sendErr)
							processingError = true
							return
						}
						r.log.Warn("Sent failed record to DLQ", "dlq_topic", r.dlqTopic,
							"topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "error", err)
						continue
					}
				}
			}
			r.log.Info("Processed", "topic", fetch.Topic, "partition", fetch.Partition)

		})

	}

	// If there was a processing error, abort transaction and return
	if processingError {
		abortCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := r.coordinator.Abort(abortCtx); err != nil {
			r.log.Error("failed to abort transaction", "error", err)
		} else {
			r.log.Info("Transaction aborted due to processing error")
		}
		return
	}

	commitCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Commit using the coordinator (handles EOS vs at-least-once)
	if r.coordinator.ShouldCommit(r.lastSuccessfulCommit, r.commitInterval) {
		if err := r.coordinator.Commit(commitCtx, r.taskManager.tasks); err != nil {
			r.log.Error("failed to commit", "error", err)
			r.changeState(StateCloseRequested)
			r.err = err
			return
		}
		r.lastSuccessfulCommit = time.Now()
		r.log.Info("Committed")
	}
}

func (r *Worker) handleCloseRequested() {
	// Use timeout context for graceful shutdown
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := r.taskManager.Close(closeCtx)
	if err != nil {
		r.log.Error("Failed to close tasks", "error", err)
	}

	// Close coordinator (handles both EOS and at-least-once cleanup)
	r.coordinator.Close()

	r.changeState(StateClosed)
}

func (r *Worker) handleClosed() {
	r.closed.Done()
}

// sendToDLQ sends a failed record to the dead letter queue topic
func (r *Worker) sendToDLQ(ctx context.Context, record *kgo.Record, originalErr error) error {
	dlqRecord := &kgo.Record{
		Topic: r.dlqTopic,
		Key:   record.Key,
		Value: record.Value,
		Headers: append(record.Headers,
			kgo.RecordHeader{Key: "kstreams.original.topic", Value: []byte(record.Topic)},
			kgo.RecordHeader{Key: "kstreams.original.partition", Value: []byte(fmt.Sprintf("%d", record.Partition))},
			kgo.RecordHeader{Key: "kstreams.original.offset", Value: []byte(fmt.Sprintf("%d", record.Offset))},
			kgo.RecordHeader{Key: "kstreams.error", Value: []byte(originalErr.Error())},
		),
	}

	results := r.coordinator.Client().ProduceSync(ctx, dlqRecord)
	if len(results) > 0 && results[0].Err != nil {
		return results[0].Err
	}
	return nil
}
