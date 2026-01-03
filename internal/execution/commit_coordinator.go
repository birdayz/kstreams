package execution

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CommitCoordinator abstracts commit coordination for different delivery semantics.
// This allows the Worker to handle EOS and at-least-once commits uniformly.
type CommitCoordinator interface {
	// Begin starts a new transaction/commit cycle. Called before processing records.
	Begin(ctx context.Context) error

	// Commit commits the current transaction/offsets. Called after successful processing.
	// The tasks slice is used to flush state stores before commit.
	Commit(ctx context.Context, tasks []*Task) error

	// Abort aborts the current transaction. Called on processing errors.
	Abort(ctx context.Context) error

	// ShouldCommit returns true if enough time has passed since last commit.
	ShouldCommit(lastCommit time.Time, interval time.Duration) bool

	// PollRecords polls records from Kafka.
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches

	// Close closes the coordinator and underlying resources.
	Close()

	// Client returns the underlying Kafka client for admin operations.
	Client() *kgo.Client
}

// eosCoordinator implements CommitCoordinator for exactly-once semantics.
type eosCoordinator struct {
	session *kgo.GroupTransactSession
	log     *slog.Logger
}

// NewEOSCoordinator creates a CommitCoordinator for exactly-once semantics.
func NewEOSCoordinator(session *kgo.GroupTransactSession, log *slog.Logger) CommitCoordinator {
	return &eosCoordinator{
		session: session,
		log:     log,
	}
}

func (c *eosCoordinator) Begin(ctx context.Context) error {
	return c.session.Begin()
}

func (c *eosCoordinator) Commit(ctx context.Context, tasks []*Task) error {
	// Flush tasks (state stores) before committing transaction
	for _, task := range tasks {
		if err := task.Flush(ctx); err != nil {
			// If flush fails, abort the transaction
			_, _ = c.session.End(ctx, kgo.TryAbort)
			return fmt.Errorf("failed to flush task: %w", err)
		}
	}

	// session.End() does three things atomically:
	// 1. Flushes the producer (ensures all records are sent)
	// 2. Commits the transaction (makes produced records visible)
	// 3. Commits consumer offsets within the transaction
	//
	// If the group rebalanced since Begin(), this will abort instead of commit
	committed, err := c.session.End(ctx, kgo.TryCommit)
	if err != nil {
		return fmt.Errorf("failed to end transaction: %w", err)
	}

	if !committed {
		// Transaction was aborted (likely due to rebalance)
		return fmt.Errorf("transaction aborted (likely due to rebalance)")
	}

	// CRITICAL: Checkpoint after successful EOS commit
	// This is essential for EOS - without it:
	// 1. Checkpoint is never written during processing
	// 2. On restart, full restoration from beginning (massive delay)
	// 3. Violates Kafka Streams' EOS checkpoint semantics
	//
	// Matches Kafka Streams' StreamTask.postCommit() in EOS mode
	for _, task := range tasks {
		if err := task.Checkpoint(ctx); err != nil {
			c.log.Error("Failed to checkpoint task after EOS commit",
				"task", task,
				"error", err)
			// Log but don't fail - transaction is already committed
			// This matches Kafka Streams behavior
		} else {
			c.log.Debug("Checkpointed task after EOS commit", "task", task)
		}
	}

	return nil
}

func (c *eosCoordinator) Abort(ctx context.Context) error {
	_, err := c.session.End(ctx, kgo.TryAbort)
	return err
}

func (c *eosCoordinator) ShouldCommit(lastCommit time.Time, interval time.Duration) bool {
	// EOS commits every batch (transaction boundary)
	return true
}

func (c *eosCoordinator) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	return c.session.PollRecords(ctx, maxRecords)
}

func (c *eosCoordinator) Close() {
	c.session.Close()
}

func (c *eosCoordinator) Client() *kgo.Client {
	return c.session.Client()
}

// atLeastOnceCoordinator implements CommitCoordinator for at-least-once semantics.
type atLeastOnceCoordinator struct {
	client      *kgo.Client
	taskManager *TaskManager
	log         *slog.Logger
}

// NewAtLeastOnceCoordinator creates a CommitCoordinator for at-least-once semantics.
func NewAtLeastOnceCoordinator(client *kgo.Client, taskManager *TaskManager, log *slog.Logger) CommitCoordinator {
	return &atLeastOnceCoordinator{
		client:      client,
		taskManager: taskManager,
		log:         log,
	}
}

func (c *atLeastOnceCoordinator) Begin(ctx context.Context) error {
	// No-op for at-least-once - no transaction to begin
	return nil
}

func (c *atLeastOnceCoordinator) Commit(ctx context.Context, tasks []*Task) error {
	if err := c.client.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}
	if err := c.taskManager.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}
	return nil
}

func (c *atLeastOnceCoordinator) Abort(ctx context.Context) error {
	// No-op for at-least-once - no transaction to abort
	return nil
}

func (c *atLeastOnceCoordinator) ShouldCommit(lastCommit time.Time, interval time.Duration) bool {
	return time.Since(lastCommit) >= interval
}

func (c *atLeastOnceCoordinator) PollRecords(ctx context.Context, maxRecords int) kgo.Fetches {
	return c.client.PollRecords(ctx, maxRecords)
}

func (c *atLeastOnceCoordinator) Close() {
	c.client.Close()
}

func (c *atLeastOnceCoordinator) Client() *kgo.Client {
	return c.client
}
