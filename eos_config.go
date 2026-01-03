package kstreams

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
