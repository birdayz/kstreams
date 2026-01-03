package kstate

// TypeErasedStoreBuilder is a non-generic interface for store builders.
// This provides type safety when storing builders in maps (avoids using 'any').
// All StoreBuilder[T] implementations must also implement this interface.
type TypeErasedStoreBuilder interface {
	// BuildStateStore constructs the state store (type-erased version)
	BuildStateStore() (StateStore, error)

	// Name returns the store name
	Name() string

	// LogConfig returns the changelog topic configuration
	LogConfig() map[string]string

	// ChangelogEnabled returns whether changelog is enabled
	ChangelogEnabled() bool

	// RestoreCallback returns the restore callback
	RestoreCallback() StateRestoreCallback
}

// StoreBuilder provides a fluent API for configuring and building state stores
// Matches Kafka Streams' org.apache.kafka.streams.state.StoreBuilder interface
//
// Key configuration options:
//   - Changelog logging (enabled/disabled per store)
//   - Changelog topic configuration (retention, cleanup policy, etc.)
//
// Example usage:
//
//	builder := pebble.NewKeyValueStoreBuilder[string, User]("user-store", "/tmp/state")
//	builder.WithChangelogEnabled(map[string]string{
//	    "retention.ms": "86400000",  // 1 day
//	    "cleanup.policy": "compact",
//	})
//	store := builder.Build()
type StoreBuilder[T StateStore] interface {
	// WithChangelogEnabled enables changelog topic for this store
	// config contains Kafka topic-level configuration (optional, can be nil)
	//
	// Common config keys:
	//   - "retention.ms": Changelog retention time in milliseconds
	//   - "cleanup.policy": "compact" or "delete"
	//   - "segment.ms": Segment size for time-based retention
	//
	// Default (if config is nil): Uses application-level defaults
	WithChangelogEnabled(config map[string]string) StoreBuilder[T]

	// WithChangelogDisabled disables changelog topic for this store
	// Use for stores that don't need durability (e.g., remote stores like S3, BigTable)
	WithChangelogDisabled() StoreBuilder[T]

	// Build constructs the state store with configured options
	// If changelog is enabled, wraps store in ChangeloggingKeyValueStore
	// Returns error if configuration is invalid or store initialization fails
	Build() (T, error)

	// Name returns the store name
	Name() string

	// LogConfig returns the changelog topic configuration
	// Returns nil if changelog is disabled
	LogConfig() map[string]string

	// ChangelogEnabled returns whether changelog is enabled for this store
	ChangelogEnabled() bool

	// RestoreCallback returns the callback for restoring this store from changelog
	// Returns nil if changelog is disabled
	RestoreCallback() StateRestoreCallback
}
