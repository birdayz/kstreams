# Changelog Topic Design for kstreams

**Status**: Design Phase
**Date**: 2025-12-30
**Based on**: Kafka Streams Java implementation (analyzed in detail)

---

## Architecture Overview

### Design Principles

1. **Stay close to Kafka Streams**: Mirror Java architecture where possible
2. **Per-store configuration**: Changelog must be optional and configurable per store
3. **Composable**: Use decorator/wrapper pattern like Java
4. **Type-safe**: Leverage Go generics
5. **Simple**: Avoid Java's complex multi-layer caching+logging stack

---

## Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    User Code (Topology)                     │
│  topology.RegisterStore("my-store", storeBuilder)           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                StoreBuilder (Configuration)                  │
│  - WithChangelogEnabled(config map[string]string)            │
│  - WithChangelogDisabled()                                   │
│  - Build() -> StateStore                                     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                    ┌──────┴──────┐
                    │LoggingEnabled?│
                    └──────┬──────┘
                           │
          ┌────────────────┴────────────────┐
          │ YES                             │ NO
          ▼                                 ▼
┌──────────────────────────┐      ┌─────────────────┐
│ChangeloggingKeyValueStore│      │   Inner Store   │
│  (Decorator/Wrapper)      │      │  (Direct use)   │
│                           │      └─────────────────┘
│  - Wraps: Inner Store     │
│  - Intercepts: Put/Delete │
│  - Logs: Via Context      │
└──────────┬────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                  ProcessorContext.LogChange()                │
│  - Gets changelog partition from StateManager                │
│  - Produces record to changelog topic via kgo.Client         │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    StateManager                              │
│  - Maps store name -> StateStoreMetadata                     │
│  - Tracks offsets per changelog partition                    │
│  - Coordinates restoration from changelog                    │
│  - Manages checkpoint file I/O                               │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│               Checkpoint File (<taskId>.checkpoint)          │
│  Format: map[TopicPartition]int64 (offsets)                 │
└─────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
kstreams/
├── store.go                      # Core interfaces (existing)
├── changelog.go                  # NEW: Changelogging interfaces
├── changelog_store.go            # NEW: ChangeloggingKeyValueStore wrapper
├── state_manager.go              # NEW: StateManager for coordination
├── checkpoint.go                 # NEW: Checkpoint file I/O
├── store_builder.go              # NEW: StoreBuilder with changelog config
├── context.go                    # MODIFIED: Add LogChange() method
├── task.go                       # MODIFIED: Add StateManager integration
├── stores/pebble/
│   ├── store_builder.go          # NEW: Pebble-specific builder
│   └── store.go                  # EXISTING: No changes needed
└── integrationtest/
    └── changelog_test.go         # NEW: Changelog tests (matching Kafka Streams)
```

---

## Key Interfaces

### 1. StateStore (Existing - No Changes)

```go
// store.go (already exists)
type StateStore interface {
    Name() string
    Init(ctx *ProcessorContext) error
    Flush(ctx context.Context) error
    Close() error
}

type KeyValueStore[K comparable, V any] interface {
    StateStore
    Get(ctx context.Context, key K) (V, bool, error)
    Set(ctx context.Context, key K, value V) error
    Delete(ctx context.Context, key K) error
    Range(ctx context.Context, from, to K) (Iterator[K, V], error)
    All(ctx context.Context) (Iterator[K, V], error)
}
```

### 2. StoreBuilder (NEW)

```go
// store_builder.go
type StoreBuilder[T StateStore] interface {
    // Configuration
    WithChangelogEnabled(config map[string]string) StoreBuilder[T]
    WithChangelogDisabled() StoreBuilder[T]

    // Build
    Build() T

    // Query
    Name() string
    LogConfig() map[string]string
    ChangelogEnabled() bool
}

// Base implementation
type baseStoreBuilder[T StateStore] struct {
    name             string
    changelogEnabled bool
    changelogConfig  map[string]string
}

func (b *baseStoreBuilder[T]) WithChangelogEnabled(config map[string]string) StoreBuilder[T] {
    b.changelogEnabled = true
    b.changelogConfig = config
    return b.(StoreBuilder[T])
}

func (b *baseStoreBuilder[T]) WithChangelogDisabled() StoreBuilder[T] {
    b.changelogEnabled = false
    b.changelogConfig = nil
    return b.(StoreBuilder[T])
}

func (b *baseStoreBuilder[T]) Name() string {
    return b.name
}

func (b *baseStoreBuilder[T]) LogConfig() map[string]string {
    return b.changelogConfig
}

func (b *baseStoreBuilder[T]) ChangelogEnabled() bool {
    return b.changelogEnabled
}
```

### 3. ChangeloggingKeyValueStore (NEW)

```go
// changelog_store.go
type ChangeloggingKeyValueStore[K comparable, V any] struct {
    inner        KeyValueStore[K, V]
    context      *ProcessorContext[K, V]
    name         string
    keyEncoder   func(K) ([]byte, error)
    valueEncoder func(V) ([]byte, error)
}

func NewChangeloggingKeyValueStore[K comparable, V any](
    inner KeyValueStore[K, V],
    keyEncoder func(K) ([]byte, error),
    valueEncoder func(V) ([]byte, error),
) *ChangeloggingKeyValueStore[K, V] {
    return &ChangeloggingKeyValueStore[K, V]{
        inner:        inner,
        keyEncoder:   keyEncoder,
        valueEncoder: valueEncoder,
    }
}

func (c *ChangeloggingKeyValueStore[K, V]) Name() string {
    return c.inner.Name()
}

func (c *ChangeloggingKeyValueStore[K, V]) Init(ctx *ProcessorContext[K, V]) error {
    c.context = ctx
    return c.inner.Init(ctx)
}

// CRITICAL: Dual-write pattern (Java: ChangeLoggingKeyValueBytesStore.put)
func (c *ChangeloggingKeyValueStore[K, V]) Set(ctx context.Context, key K, value V) error {
    // 1. Write to inner store FIRST
    if err := c.inner.Set(ctx, key, value); err != nil {
        return err
    }

    // 2. Log change to changelog topic
    return c.log(key, &value)
}

// Deletion logs with nil value (tombstone)
func (c *ChangeloggingKeyValueStore[K, V]) Delete(ctx context.Context, key K) error {
    // 1. Delete from inner store FIRST
    if err := c.inner.Delete(ctx, key); err != nil {
        return err
    }

    // 2. Log deletion (nil value = tombstone)
    return c.log(key, nil)
}

// Read operations pass through (no logging)
func (c *ChangeloggingKeyValueStore[K, V]) Get(ctx context.Context, key K) (V, bool, error) {
    return c.inner.Get(ctx, key)
}

func (c *ChangeloggingKeyValueStore[K, V]) Range(ctx context.Context, from, to K) (Iterator[K, V], error) {
    return c.inner.Range(ctx, from, to)
}

func (c *ChangeloggingKeyValueStore[K, V]) All(ctx context.Context) (Iterator[K, V], error) {
    return c.inner.All(ctx)
}

func (c *ChangeloggingKeyValueStore[K, V]) Flush(ctx context.Context) error {
    return c.inner.Flush(ctx)
}

func (c *ChangeloggingKeyValueStore[K, V]) Close() error {
    return c.inner.Close()
}

// Log change via context (matches Java: ChangeLoggingKeyValueBytesStore.log)
func (c *ChangeloggingKeyValueStore[K, V]) log(key K, value *V) error {
    keyBytes, err := c.keyEncoder(key)
    if err != nil {
        return fmt.Errorf("encode key: %w", err)
    }

    var valueBytes []byte
    if value != nil {
        valueBytes, err = c.valueEncoder(*value)
        if err != nil {
            return fmt.Errorf("encode value: %w", err)
        }
    } // nil value = tombstone, leave valueBytes as nil

    return c.context.LogChange(c.name, keyBytes, valueBytes)
}
```

### 4. StateManager (NEW)

```go
// state_manager.go
type StateStoreMetadata struct {
    Store             StateStore
    ChangelogPartition *TopicPartition // nil if not logged
    Offset            *int64          // nil = unknown
    RestoreCallback   StateRestoreCallback
    Corrupted         bool
}

type StateManager struct {
    taskID            string
    stores            map[string]*StateStoreMetadata
    client            *kgo.Client
    checkpointFile    *CheckpointFile
    stateDir          string
}

func NewStateManager(taskID string, stateDir string, client *kgo.Client) *StateManager {
    return &StateManager{
        taskID:         taskID,
        stores:         make(map[string]*StateStoreMetadata),
        client:         client,
        checkpointFile: NewCheckpointFile(filepath.Join(stateDir, fmt.Sprintf("%s.checkpoint", taskID))),
        stateDir:       stateDir,
    }
}

// Register a store (called during task initialization)
func (sm *StateManager) RegisterStore(
    store StateStore,
    changelogPartition *TopicPartition, // nil if logging disabled
    restoreCallback StateRestoreCallback,
) error {
    metadata := &StateStoreMetadata{
        Store:              store,
        ChangelogPartition: changelogPartition,
        RestoreCallback:    restoreCallback,
        Corrupted:          false,
    }

    sm.stores[store.Name()] = metadata
    return nil
}

// Initialize offsets from checkpoint file
func (sm *StateManager) InitializeOffsetsFromCheckpoint() error {
    checkpoints, err := sm.checkpointFile.Read()
    if err != nil && !os.IsNotExist(err) {
        return fmt.Errorf("read checkpoint: %w", err)
    }

    for storeName, metadata := range sm.stores {
        if metadata.ChangelogPartition == nil {
            continue // Not logged
        }

        if offset, ok := checkpoints[*metadata.ChangelogPartition]; ok {
            metadata.Offset = &offset
            slog.Info("Loaded checkpoint offset",
                "store", storeName,
                "partition", metadata.ChangelogPartition,
                "offset", offset)
        } else {
            // No checkpoint: start from beginning
            metadata.Offset = nil
            slog.Info("No checkpoint found, will restore from beginning",
                "store", storeName,
                "partition", metadata.ChangelogPartition)
        }
    }

    return nil
}

// Restore a batch of records to a store
func (sm *StateManager) Restore(storeName string, records []*kgo.Record) error {
    metadata, ok := sm.stores[storeName]
    if !ok {
        return fmt.Errorf("unknown store: %s", storeName)
    }

    if metadata.RestoreCallback == nil {
        return fmt.Errorf("no restore callback for store: %s", storeName)
    }

    // Restore batch
    if err := metadata.RestoreCallback.RestoreBatch(records); err != nil {
        return fmt.Errorf("restore batch: %w", err)
    }

    // Update offset to last record in batch
    if len(records) > 0 {
        lastOffset := records[len(records)-1].Offset
        metadata.Offset = &lastOffset
    }

    return nil
}

// Checkpoint: Write offsets to disk
func (sm *StateManager) Checkpoint(ctx context.Context) error {
    checkpoints := make(map[TopicPartition]int64)

    for storeName, metadata := range sm.stores {
        if metadata.ChangelogPartition == nil {
            continue // Not logged
        }

        if metadata.Corrupted {
            continue // Don't checkpoint corrupted stores
        }

        if metadata.Offset != nil {
            checkpoints[*metadata.ChangelogPartition] = *metadata.Offset
        } else {
            // Unknown offset: use sentinel -1
            checkpoints[*metadata.ChangelogPartition] = -1
        }
    }

    return sm.checkpointFile.Write(checkpoints)
}

// Get changelog partition for a store
func (sm *StateManager) ChangelogPartitionFor(storeName string) *TopicPartition {
    metadata, ok := sm.stores[storeName]
    if !ok {
        return nil
    }
    return metadata.ChangelogPartition
}
```

### 5. CheckpointFile (NEW)

```go
// checkpoint.go
type TopicPartition struct {
    Topic     string
    Partition int32
}

type CheckpointFile struct {
    path string
}

func NewCheckpointFile(path string) *CheckpointFile {
    return &CheckpointFile{path: path}
}

// Read checkpoint file (format: JSON for simplicity)
func (c *CheckpointFile) Read() (map[TopicPartition]int64, error) {
    data, err := os.ReadFile(c.path)
    if err != nil {
        return nil, err
    }

    var checkpoints map[TopicPartition]int64
    if err := json.Unmarshal(data, &checkpoints); err != nil {
        return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
    }

    return checkpoints, nil
}

// Write checkpoint file atomically
func (c *CheckpointFile) Write(checkpoints map[TopicPartition]int64) error {
    data, err := json.MarshalIndent(checkpoints, "", "  ")
    if err != nil {
        return fmt.Errorf("marshal checkpoint: %w", err)
    }

    // Write atomically (write to temp, then rename)
    tmpPath := c.path + ".tmp"
    if err := os.WriteFile(tmpPath, data, 0644); err != nil {
        return fmt.Errorf("write temp checkpoint: %w", err)
    }

    if err := os.Rename(tmpPath, c.path); err != nil {
        return fmt.Errorf("rename checkpoint: %w", err)
    }

    return nil
}
```

### 6. StateRestoreCallback (NEW)

```go
// changelog.go
type StateRestoreCallback interface {
    Restore(key, value []byte) error
    RestoreBatch(records []*kgo.Record) error
}

// Default implementation for KeyValueStore
type KeyValueStoreRestoreCallback[K comparable, V any] struct {
    store        KeyValueStore[K, V]
    keyDecoder   func([]byte) (K, error)
    valueDecoder func([]byte) (V, error)
}

func (r *KeyValueStoreRestoreCallback[K, V]) Restore(key, value []byte) error {
    k, err := r.keyDecoder(key)
    if err != nil {
        return fmt.Errorf("decode key: %w", err)
    }

    if value == nil {
        // Tombstone: delete
        return r.store.Delete(context.Background(), k)
    }

    v, err := r.valueDecoder(value)
    if err != nil {
        return fmt.Errorf("decode value: %w", err)
    }

    return r.store.Set(context.Background(), k, v)
}

func (r *KeyValueStoreRestoreCallback[K, V]) RestoreBatch(records []*kgo.Record) error {
    for _, record := range records {
        if err := r.Restore(record.Key, record.Value); err != nil {
            return err
        }
    }
    return nil
}
```

### 7. ProcessorContext.LogChange() (MODIFY EXISTING)

```go
// context.go (add this method)
func (c *ProcessorContext[K, V]) LogChange(storeName string, key, value []byte) error {
    // Get changelog partition from state manager
    changelogPartition := c.task.stateManager.ChangelogPartitionFor(storeName)
    if changelogPartition == nil {
        return fmt.Errorf("no changelog partition for store: %s", storeName)
    }

    // Produce to changelog topic
    record := &kgo.Record{
        Topic:     changelogPartition.Topic,
        Partition: changelogPartition.Partition,
        Key:       key,
        Value:     value,
        Timestamp: time.Now(), // Could use record timestamp from context
    }

    // Synchronous produce for simplicity (could be async with callback)
    return c.task.client.ProduceSync(context.Background(), record).FirstErr()
}
```

---

## Changelog Topic Naming Convention

Matches Kafka Streams exactly:

```
Format: <application-id>-<store-name>-changelog

Examples:
  my-app-user-store-changelog
  my-app-session-store-changelog
```

Partition mapping:
```
Changelog partition = Task partition = Input topic partition
```

---

## Configuration Model

### Per-Store Configuration (Builder Pattern)

```go
// Example: Pebble store with changelog enabled
storeBuilder := pebble.NewKeyValueStoreBuilder[string, User](
    "user-store",
    "/tmp/state",
).WithChangelogEnabled(map[string]string{
    "retention.ms": "86400000",  // 1 day
    "cleanup.policy": "compact",
})

store := storeBuilder.Build()
```

### Per-Store Configuration (Disable Changelog)

```go
// Example: S3-backed store WITHOUT changelog (as requested)
storeBuilder := s3.NewKeyValueStoreBuilder[string, User](
    "s3-store",
    s3Config,
).WithChangelogDisabled()  // No changelog for S3!

store := storeBuilder.Build()
```

### Default Behavior

Following Kafka Streams:
- **Default**: Changelog ENABLED for persistent stores (Pebble)
- **User override**: Can disable via `WithChangelogDisabled()`
- **Remote stores**: Should disable by default (S3, BigTable, etc.)

---

## Restoration Flow

```
Task Initialization:
  1. StateManager.InitializeOffsetsFromCheckpoint()
       -> Load offsets from <taskId>.checkpoint file
       -> If no checkpoint: offset = nil (start from beginning)

  2. For each logged store:
       -> Subscribe to changelog topic-partition
       -> Start consuming from (checkpoint_offset + 1) or offset 0

  3. Restoration Loop:
       -> Poll changelog topic in batches (1000 records)
       -> StateManager.Restore(storeName, records)
            -> restoreCallback.RestoreBatch(records)
                 -> For each record:
                      if value == nil: store.Delete(key)
                      else: store.Set(key, value)
            -> Update metadata.Offset = lastRecord.Offset
       -> Repeat until caught up (offset == high water mark)

  4. After restoration complete:
       -> Transition task to RUNNING state
       -> Start processing input topic records

  5. On each commit:
       -> StateManager.Checkpoint()
       -> Write offsets to disk atomically
```

---

## Testing Strategy

Match Kafka Streams test cases:

### Test Cases to Implement

1. **TestChangelogDualWrite**
   - Write to store
   - Verify changelog topic has matching record

2. **TestChangelogRestoration**
   - Write to store (creates changelog)
   - Close app
   - Restart app
   - Verify state restored from changelog (not input topic)

3. **TestChangelogDeletion**
   - Set key=value
   - Delete key
   - Verify changelog has tombstone (null value)

4. **TestCheckpointPersistence**
   - Process records
   - Commit (writes checkpoint)
   - Verify checkpoint file contains correct offsets

5. **TestRestorationFromCheckpoint**
   - Process 100 records (checkpoint offset=99)
   - Crash (no shutdown)
   - Restart
   - Verify restoration starts from offset 100 (not 0)

6. **TestChangelogDisabled**
   - Create store with WithChangelogDisabled()
   - Write to store
   - Verify NO changelog topic created

7. **TestPartitionMapping**
   - 3 input partitions
   - 3 changelog partitions
   - Verify partition N writes to changelog partition N

---

## Implementation Order

1. ✅ `checkpoint.go` - Simple file I/O, no dependencies
2. ✅ `changelog.go` - Interfaces and restore callback
3. ✅ `store_builder.go` - Builder interface
4. ✅ `changelog_store.go` - Wrapper decorator
5. ✅ `state_manager.go` - Coordination logic
6. ✅ `context.go` - Add LogChange() method
7. ✅ `task.go` - Integrate StateManager
8. ✅ `stores/pebble/store_builder.go` - Concrete builder
9. ✅ `integrationtest/changelog_test.go` - Tests

---

## Differences from Kafka Streams

| Feature | Kafka Streams (Java) | kstreams (Go) | Reason |
|---------|---------------------|---------------|--------|
| **Caching layer** | ✅ Separate wrapper | ❌ Not implemented | Simplicity (can add later) |
| **Composite wrapping** | Caching + Logging | Only Logging | Focus on changelog first |
| **Window store changelog** | Special key encoding | Future work | Start with KV stores |
| **Session store changelog** | Special key encoding | Future work | Start with KV stores |
| **LRU eviction logging** | ✅ Supported | Future work | Start with basic operations |
| **Position tracking** | ✅ IQ consistency | ❌ Not needed yet | Future for IQ |
| **Standby tasks** | ✅ Passive replicas | Future work | Focus on active tasks |
| **Restoration listener** | ✅ Progress callbacks | Future work | Start with basic restoration |

---

## Summary

This design stays **very close to Kafka Streams** while adapting to Go idioms:

✅ **Same patterns**: Decorator, StateManager, Checkpoint file, Dual-write
✅ **Same lifecycle**: Register, Initialize, Restore, Log, Checkpoint
✅ **Same configuration**: Per-store enable/disable
✅ **Same topic naming**: `<app-id>-<store-name>-changelog`
✅ **Same partition mapping**: Changelog partition = Task partition

Ready to implement!
