package coordination

// EOSConfig holds the configuration for exactly-once semantics
type EOSConfig struct {
	// Enabled determines if exactly-once semantics is enabled
	Enabled bool

	// TransactionalID is the base transactional ID for this application
	// Each task will append its task ID to this base
	TransactionalID string
}
