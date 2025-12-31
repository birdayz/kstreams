package kstreams

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
	Build() T

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

// BaseStoreBuilder provides common functionality for all store builders
// Concrete builders embed this and implement Build()
type BaseStoreBuilder[T StateStore] struct {
	name             string
	changelogEnabled bool
	changelogConfig  map[string]string
	restoreCallback  StateRestoreCallback
}

// NewBaseStoreBuilder creates a new base store builder
// Default: changelog ENABLED (matches Kafka Streams default for persistent stores)
func NewBaseStoreBuilder[T StateStore](name string) *BaseStoreBuilder[T] {
	return &BaseStoreBuilder[T]{
		name:             name,
		changelogEnabled: true, // Default: enabled (Kafka Streams behavior)
		changelogConfig:  make(map[string]string),
	}
}

// WithChangelogEnabled enables changelog with optional topic configuration
func (b *BaseStoreBuilder[T]) WithChangelogEnabled(config map[string]string) *BaseStoreBuilder[T] {
	b.changelogEnabled = true
	if config != nil {
		b.changelogConfig = config
	} else {
		b.changelogConfig = make(map[string]string)
	}
	return b
}

// WithChangelogDisabled disables changelog
func (b *BaseStoreBuilder[T]) WithChangelogDisabled() *BaseStoreBuilder[T] {
	b.changelogEnabled = false
	b.changelogConfig = nil
	b.restoreCallback = nil
	return b
}

// Name returns the store name
func (b *BaseStoreBuilder[T]) Name() string {
	return b.name
}

// LogConfig returns the changelog topic configuration
func (b *BaseStoreBuilder[T]) LogConfig() map[string]string {
	return b.changelogConfig
}

// ChangelogEnabled returns whether changelog is enabled
func (b *BaseStoreBuilder[T]) ChangelogEnabled() bool {
	return b.changelogEnabled
}

// RestoreCallback returns the restore callback
func (b *BaseStoreBuilder[T]) RestoreCallback() StateRestoreCallback {
	return b.restoreCallback
}

// SetRestoreCallback is used by concrete builders to set the restore callback
func (b *BaseStoreBuilder[T]) SetRestoreCallback(callback StateRestoreCallback) {
	b.restoreCallback = callback
}
