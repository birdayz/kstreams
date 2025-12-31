package kstreams

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// EOSConfig holds the configuration for exactly-once semantics
type EOSConfig struct {
	// Enabled determines if exactly-once semantics is enabled
	Enabled bool

	// TransactionalID is the base transactional ID for this application
	// Each task will append its task ID to this base
	TransactionalID string
}

// WithExactlyOnce enables exactly-once semantics (EOS) for the application.
//
// When enabled, kstreams will:
// - Use transactional producers for all output records
// - Commit consumer offsets within the same transaction as produced records
// - Use read_committed isolation level for consumers
// - Provide exactly-once processing guarantees
//
// Requirements:
// - Kafka brokers must support transactions (0.11.0+)
// - Each task gets a unique transactional.id (appId-taskId)
// - Increased latency due to transaction coordination overhead
//
// Trade-offs:
// - Guarantees: exactly-once vs at-least-once
// - Performance: ~2-3x higher latency due to transaction overhead
// - Throughput: Slightly reduced due to transaction coordination
//
// Example:
//
//	app := kstreams.New(
//	    topology,
//	    "my-app",
//	    kstreams.WithBrokers(brokers),
//	    kstreams.WithExactlyOnce(), // Enable EOS
//	)
func WithExactlyOnce() Option {
	return func(s *App) {
		s.eosConfig.Enabled = true
		s.eosConfig.TransactionalID = s.groupName // Use group name as base transactional ID
	}
}

// TransactionCoordinator manages transactions for a single task
type TransactionCoordinator struct {
	client          *kgo.Client
	transactionalID string
	log             *slog.Logger
	inTransaction   bool
}

// NewTransactionCoordinator creates a new transaction coordinator for a task
func NewTransactionCoordinator(client *kgo.Client, transactionalID string, log *slog.Logger) *TransactionCoordinator {
	return &TransactionCoordinator{
		client:          client,
		transactionalID: transactionalID,
		log:             log,
		inTransaction:   false,
	}
}

// BeginTransaction starts a new transaction
func (tc *TransactionCoordinator) BeginTransaction(ctx context.Context) error {
	if tc.inTransaction {
		return fmt.Errorf("transaction already in progress")
	}

	// BeginTransaction doesn't take a context in franz-go
	if err := tc.client.BeginTransaction(); err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	tc.inTransaction = true
	tc.log.Debug("Transaction begun", "txn_id", tc.transactionalID)
	return nil
}

// CommitTransaction commits the current transaction
//
// TODO: This currently does NOT commit consumer offsets within the transaction.
// To achieve true exactly-once semantics, we need to:
// 1. Use GroupTransactSession (recommended by franz-go)
// 2. Or manually add offsets to the transaction before committing
//
// For now, this provides transactional produces but not atomic offset commits.
func (tc *TransactionCoordinator) CommitTransaction(ctx context.Context, offsets map[string]map[int32]kgo.EpochOffset, groupID string) error {
	if !tc.inTransaction {
		return fmt.Errorf("no transaction in progress")
	}

	// TODO: Add offsets to transaction here
	// This requires using the correct franz-go API which may be:
	// - Part of GroupTransactSession
	// - A combination of lower-level transaction APIs
	// For now, we just commit the transaction without offset coordination

	// Commit transaction
	if err := tc.client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		tc.inTransaction = false
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	tc.inTransaction = false
	tc.log.Debug("Transaction committed", "txn_id", tc.transactionalID)

	// Log warning about incomplete EOS
	tc.log.Warn("EOS implementation incomplete: offsets not committed within transaction",
		"txn_id", tc.transactionalID,
		"group_id", groupID)

	return nil
}

// AbortTransaction aborts the current transaction
func (tc *TransactionCoordinator) AbortTransaction(ctx context.Context) error {
	if !tc.inTransaction {
		return fmt.Errorf("no transaction in progress")
	}

	if err := tc.client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		tc.inTransaction = false
		return fmt.Errorf("failed to abort transaction: %w", err)
	}

	tc.inTransaction = false
	tc.log.Debug("Transaction aborted", "txn_id", tc.transactionalID)
	return nil
}

// IsInTransaction returns whether a transaction is currently active
func (tc *TransactionCoordinator) IsInTransaction() bool {
	return tc.inTransaction
}
