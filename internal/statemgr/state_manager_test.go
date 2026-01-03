package statemgr

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams/kprocessor"
)

// Mock StateStore for testing
type mockStateStore struct {
	name       string
	persistent bool
	initCalled bool
	flushErr   error
	closeErr   error
}

func newMockStateStore(name string, persistent bool) *mockStateStore {
	return &mockStateStore{
		name:       name,
		persistent: persistent,
	}
}

func (m *mockStateStore) Name() string {
	return m.name
}

func (m *mockStateStore) Persistent() bool {
	return m.persistent
}

func (m *mockStateStore) Init(ctx kprocessor.ProcessorContextInternal) error {
	m.initCalled = true
	return nil
}

func (m *mockStateStore) Flush(ctx context.Context) error {
	return m.flushErr
}

func (m *mockStateStore) Close() error {
	return m.closeErr
}

// Mock CommitCallback for testing
type mockCommitCallback struct {
	onCommitCalled int
	onCommitErr    error
}

func (m *mockCommitCallback) OnCommit() error {
	m.onCommitCalled++
	return m.onCommitErr
}

func TestStateManager_NewStateManager(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	if sm == nil {
		t.Fatal("Expected StateManager to be created")
	}

	if sm.taskID != "task-0" {
		t.Errorf("Expected taskID 'task-0', got '%s'", sm.taskID)
	}

	if sm.partition != 0 {
		t.Errorf("Expected partition 0, got %d", sm.partition)
	}

	if sm.stateDir == "" {
		t.Error("Expected stateDir to be set")
	}
}

func TestStateManager_RegisterStore(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)

	// Register with changelog enabled
	err := sm.RegisterStore(store, true, nil, nil)
	if err != nil {
		t.Fatalf("Failed to register store: %v", err)
	}

	// Verify store is registered
	metadata, ok := sm.stores["test-store"]
	if !ok {
		t.Fatal("Store not registered")
	}

	if metadata.Store.Name() != "test-store" {
		t.Errorf("Expected store name 'test-store', got '%s'", metadata.Store.Name())
	}

	if metadata.ChangelogPartition == nil {
		t.Error("Expected changelog partition to be set")
	}

	expectedTopic := "test-app-test-store-changelog"
	if metadata.ChangelogPartition.Topic != expectedTopic {
		t.Errorf("Expected changelog topic '%s', got '%s'",
			expectedTopic, metadata.ChangelogPartition.Topic)
	}

	if metadata.ChangelogPartition.Partition != 0 {
		t.Errorf("Expected changelog partition 0, got %d",
			metadata.ChangelogPartition.Partition)
	}
}

func TestStateManager_RegisterStore_NoChangelog(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)

	// Register with changelog disabled
	err := sm.RegisterStore(store, false, nil, nil)
	if err != nil {
		t.Fatalf("Failed to register store: %v", err)
	}

	metadata, ok := sm.stores["test-store"]
	if !ok {
		t.Fatal("Store not registered")
	}

	if metadata.ChangelogPartition != nil {
		t.Error("Expected changelog partition to be nil for changelog-disabled store")
	}
}

func TestStateManager_RegisterStore_DuplicateName(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store1 := newMockStateStore("test-store", true)
	store2 := newMockStateStore("test-store", true)

	// Register first store
	err := sm.RegisterStore(store1, true, nil, nil)
	if err != nil {
		t.Fatalf("Failed to register first store: %v", err)
	}

	// Register second store with same name (should error)
	err = sm.RegisterStore(store2, true, nil, nil)
	if err == nil {
		t.Fatal("Expected error when registering duplicate store name")
	}
}

func TestStateManager_AcquireLock(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	// Lock should succeed
	err := sm.AcquireLock()
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Verify lock file exists
	lockPath := filepath.Join(sm.stateDir, ".lock")
	if _, err := os.Stat(lockPath); os.IsNotExist(err) {
		t.Error("Lock file should exist")
	}

	// Clean up
	assert.NoError(t, sm.directoryLock.Unlock())
}

func TestStateManager_AcquireLock_DoubleLock(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	// First lock should succeed
	assert.NoError(t, sm.AcquireLock())

	// Second lock should fail
	err := sm.AcquireLock()
	assert.Error(t, err)

	// Clean up
	assert.NoError(t, sm.directoryLock.Unlock())
}

func TestStateManager_AcquireLock_Concurrent(t *testing.T) {
	tmpDir := t.TempDir()

	sm1 := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)
	sm2 := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	// First StateManager acquires lock
	assert.NoError(t, sm1.AcquireLock())

	// Second StateManager should fail to acquire lock
	err := sm2.AcquireLock()
	assert.Error(t, err)

	// Release first lock
	assert.NoError(t, sm1.directoryLock.Unlock())

	// Now second StateManager should succeed
	assert.NoError(t, sm2.AcquireLock())
	assert.NoError(t, sm2.directoryLock.Unlock())
}

func TestStateManager_UnlockDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	// Acquire lock
	assert.NoError(t, sm.AcquireLock())

	lockPath := filepath.Join(sm.stateDir, ".lock")

	// Verify lock file exists
	if _, err := os.Stat(lockPath); os.IsNotExist(err) {
		t.Error("Lock file should exist before release")
	}

	// Release lock
	assert.NoError(t, sm.directoryLock.Unlock())

	// Verify lock file is removed
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Error("Lock file should be removed after release")
	}
}

func TestStateManager_InitializeOffsetsFromCheckpoint_NoCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Initialize without existing checkpoint
	err := sm.InitializeOffsetsFromCheckpoint(false)
	if err != nil {
		t.Fatalf("Failed to initialize offsets: %v", err)
	}

	// Offset should be nil (unknown)
	metadata := sm.stores["test-store"]
	if metadata.Offset != nil {
		t.Errorf("Expected offset to be nil, got %d", *metadata.Offset)
	}
}

func TestStateManager_InitializeOffsetsFromCheckpoint_WithCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Write checkpoint file
	checkpoints := map[TopicPartition]int64{
		{Topic: "test-app-test-store-changelog", Partition: 0}: 12345,
	}
	err := sm.checkpointFile.Write(checkpoints)
	if err != nil {
		t.Fatalf("Failed to write checkpoint: %v", err)
	}

	// Initialize from checkpoint
	err = sm.InitializeOffsetsFromCheckpoint(false)
	if err != nil {
		t.Fatalf("Failed to initialize offsets: %v", err)
	}

	// Offset should be loaded from checkpoint
	metadata := sm.stores["test-store"]
	if metadata.Offset == nil {
		t.Fatal("Expected offset to be loaded from checkpoint")
	}
	if *metadata.Offset != 12345 {
		t.Errorf("Expected offset 12345, got %d", *metadata.Offset)
	}
}

func TestStateManager_InitializeOffsetsFromCheckpoint_OFFSET_UNKNOWN(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Write checkpoint with OFFSET_UNKNOWN
	checkpoints := map[TopicPartition]int64{
		{Topic: "test-app-test-store-changelog", Partition: 0}: OFFSET_UNKNOWN,
	}
	err := sm.checkpointFile.Write(checkpoints)
	if err != nil {
		t.Fatalf("Failed to write checkpoint: %v", err)
	}

	// Initialize from checkpoint
	err = sm.InitializeOffsetsFromCheckpoint(false)
	if err != nil {
		t.Fatalf("Failed to initialize offsets: %v", err)
	}

	// Offset should be nil for OFFSET_UNKNOWN
	metadata := sm.stores["test-store"]
	if metadata.Offset != nil {
		t.Errorf("Expected offset to be nil for OFFSET_UNKNOWN, got %d", *metadata.Offset)
	}
}

func TestStateManager_Flush(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store1 := newMockStateStore("store1", true)
	store2 := newMockStateStore("store2", true)

	assert.NoError(t, sm.RegisterStore(store1, true, nil, nil))
	assert.NoError(t, sm.RegisterStore(store2, true, nil, nil))

	// Flush should call Flush() on all stores
	err := sm.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
}

func TestStateManager_Flush_WithError(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("store1", true)
	store.flushErr = os.ErrInvalid // Simulate flush error

	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Flush should return error from store
	err := sm.Flush(context.Background())
	if err == nil {
		t.Fatal("Expected flush to return error")
	}
}

func TestStateManager_Flush_SkipsCorruptedStores(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("store1", true)
	store.flushErr = os.ErrInvalid

	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Mark store as corrupted
	sm.stores["store1"].Corrupted = true

	// Flush should skip corrupted store
	err := sm.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush should skip corrupted stores, got error: %v", err)
	}
}

func TestStateManager_Checkpoint(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Set offset
	offset := int64(12345)
	sm.stores["test-store"].Offset = &offset

	// Checkpoint
	err := sm.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Verify checkpoint file was written
	checkpoints, err := sm.checkpointFile.Read()
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	tp := TopicPartition{Topic: "test-app-test-store-changelog", Partition: 0}
	if checkpoints[tp] != 12345 {
		t.Errorf("Expected checkpoint offset 12345, got %d", checkpoints[tp])
	}
}

func TestStateManager_Checkpoint_WithCommitCallback(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	callback := &mockCommitCallback{}

	assert.NoError(t, sm.RegisterStore(store, true, nil, callback))

	// Set offset
	offset := int64(100)
	sm.stores["test-store"].Offset = &offset

	// Checkpoint should call CommitCallback
	err := sm.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	if callback.onCommitCalled != 1 {
		t.Errorf("Expected OnCommit to be called once, got %d", callback.onCommitCalled)
	}
}

func TestStateManager_Checkpoint_CommitCallbackError(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	callback := &mockCommitCallback{
		onCommitErr: os.ErrInvalid,
	}

	assert.NoError(t, sm.RegisterStore(store, true, nil, callback))

	// Checkpoint should fail if CommitCallback errors
	err := sm.Checkpoint(context.Background())
	if err == nil {
		t.Fatal("Expected checkpoint to fail when CommitCallback errors")
	}
}

func TestStateManager_Checkpoint_SkipsNonPersistent(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	// Non-persistent store
	store := newMockStateStore("test-store", false)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Set offset
	offset := int64(12345)
	sm.stores["test-store"].Offset = &offset

	// Checkpoint should skip non-persistent stores
	err := sm.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Verify checkpoint file is empty (or doesn't exist)
	checkpoints, err := sm.checkpointFile.Read()
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	if len(checkpoints) != 0 {
		t.Errorf("Expected empty checkpoint for non-persistent store, got %d entries", len(checkpoints))
	}
}

func TestStateManager_Checkpoint_SkipsCorrupted(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Mark as corrupted
	sm.stores["test-store"].Corrupted = true

	// Set offset
	offset := int64(12345)
	sm.stores["test-store"].Offset = &offset

	// Checkpoint should skip corrupted stores
	err := sm.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Verify checkpoint file is empty
	checkpoints, err := sm.checkpointFile.Read()
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	if len(checkpoints) != 0 {
		t.Errorf("Expected empty checkpoint for corrupted store, got %d entries", len(checkpoints))
	}
}

func TestStateManager_CheckpointNeeded(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Initially no last checkpointed offsets - first checkpoint SHOULD be written
	// to establish a baseline. This is critical for recovery.
	if !sm.CheckpointNeeded(false) {
		t.Error("Expected CheckpointNeeded to return true when no baseline exists (first checkpoint)")
	}

	// Set current offset
	offset := int64(1000)
	sm.stores["test-store"].Offset = &offset

	// Do a checkpoint
	assert.NoError(t, sm.Checkpoint(context.Background()))

	// Update offset by small amount (< 10k threshold)
	offset = 1500
	sm.stores["test-store"].Offset = &offset

	// Should not need checkpoint
	if sm.CheckpointNeeded(false) {
		t.Error("Expected CheckpointNeeded to return false when delta < threshold")
	}

	// Update offset by large amount (> 10k threshold)
	offset = 15000
	sm.stores["test-store"].Offset = &offset

	// Should need checkpoint
	if !sm.CheckpointNeeded(false) {
		t.Error("Expected CheckpointNeeded to return true when delta > threshold")
	}
}

func TestStateManager_CheckpointNeeded_EnforceCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Do a checkpoint
	offset := int64(1000)
	sm.stores["test-store"].Offset = &offset
	assert.NoError(t, sm.Checkpoint(context.Background()))

	// Update offset by tiny amount
	offset = 1001
	sm.stores["test-store"].Offset = &offset

	// With enforceCheckpoint=true, should always need checkpoint
	if !sm.CheckpointNeeded(true) {
		t.Error("Expected CheckpointNeeded to return true when enforced")
	}
}

func TestStateManager_DeleteCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Write checkpoint
	offset := int64(12345)
	sm.stores["test-store"].Offset = &offset
	assert.NoError(t, sm.Checkpoint(context.Background()))

	checkpointPath := filepath.Join(sm.stateDir, "task-0.checkpoint")

	// Verify checkpoint exists
	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatal("Checkpoint file should exist")
	}

	// Delete checkpoint
	err := sm.DeleteCheckpoint()
	if err != nil {
		t.Fatalf("Failed to delete checkpoint: %v", err)
	}

	// Verify checkpoint is deleted
	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Error("Checkpoint file should be deleted")
	}
}

func TestStateManager_SetOffset(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Set offset
	assert.NoError(t, sm.SetOffset("test-store", 12345))

	// Verify offset is set
	metadata := sm.stores["test-store"]
	if metadata.Offset == nil {
		t.Fatal("Expected offset to be set")
	}
	if *metadata.Offset != 12345 {
		t.Errorf("Expected offset 12345, got %d", *metadata.Offset)
	}
}

func TestStateManager_SetOffset_NonExistentStore(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	// SetOffset on non-existent store should return error
	err := sm.SetOffset("non-existent", 12345)
	if err == nil {
		t.Error("Expected error for non-existent store, got nil")
	}
}

func TestStateManager_ChangelogPartitionFor(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Get changelog partition
	partition := sm.ChangelogPartitionFor("test-store")
	if partition == nil {
		t.Fatal("Expected changelog partition to be returned")
	}

	expectedTopic := "test-app-test-store-changelog"
	if partition.Topic != expectedTopic {
		t.Errorf("Expected topic '%s', got '%s'", expectedTopic, partition.Topic)
	}

	if partition.Partition != 0 {
		t.Errorf("Expected partition 0, got %d", partition.Partition)
	}
}

func TestStateManager_ChangelogPartitionFor_NoChangelog(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, false, nil, nil)) // Changelog disabled

	// Should return nil for stores without changelog
	partition := sm.ChangelogPartitionFor("test-store")
	if partition != nil {
		t.Error("Expected nil for store without changelog")
	}
}

func TestStateManager_ChangelogPartitionFor_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	// Should return nil for non-existent store
	partition := sm.ChangelogPartitionFor("non-existent")
	if partition != nil {
		t.Error("Expected nil for non-existent store")
	}
}

func TestStateManager_MarkChangelogAsCorrupted(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Initially not corrupted
	if sm.stores["test-store"].Corrupted {
		t.Error("Store should not be corrupted initially")
	}

	// Mark as corrupted
	tp := TopicPartition{Topic: "test-app-test-store-changelog", Partition: 0}
	sm.MarkChangelogAsCorrupted([]TopicPartition{tp})

	// Verify corrupted flag is set
	if !sm.stores["test-store"].Corrupted {
		t.Error("Store should be marked as corrupted")
	}
}

func TestStateManager_UpdateChangelogOffsets(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Update offsets
	offsets := map[TopicPartition]int64{
		{Topic: "test-app-test-store-changelog", Partition: 0}: 12345,
	}

	sm.UpdateChangelogOffsets(offsets)

	// Verify offset is updated
	metadata := sm.stores["test-store"]
	if metadata.Offset == nil {
		t.Fatal("Expected offset to be set")
	}
	if *metadata.Offset != 12345 {
		t.Errorf("Expected offset 12345, got %d", *metadata.Offset)
	}
}

func TestStateManager_Close(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store1 := newMockStateStore("store1", true)
	store2 := newMockStateStore("store2", true)

	assert.NoError(t, sm.RegisterStore(store1, true, nil, nil))
	assert.NoError(t, sm.RegisterStore(store2, true, nil, nil))

	// Acquire lock
	assert.NoError(t, sm.AcquireLock())

	// Close should close all stores and release lock
	err := sm.Close(context.Background())
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	lockPath := filepath.Join(sm.stateDir, ".lock")

	// Verify lock is released
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Error("Lock should be released after close")
	}
}

func TestStateManager_Close_WithError(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("store1", true)
	store.closeErr = os.ErrInvalid

	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Close should return error from store but still release lock
	err := sm.Close(context.Background())
	if err == nil {
		t.Fatal("Expected close to return error")
	}

	lockPath := filepath.Join(sm.stateDir, ".lock")

	// Lock should still be released
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Error("Lock should be released even after close error")
	}
}

// TestStateManager_Close_FlushError verifies that Close returns flush errors
// (not just store close errors)
func TestStateManager_Close_FlushError(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("store1", true)
	store.flushErr = os.ErrInvalid // Flush will fail

	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))
	assert.NoError(t, sm.AcquireLock())

	// Close should return the flush error
	err := sm.Close(context.Background())
	if err == nil {
		t.Fatal("Expected close to return flush error, got nil")
	}

	// Lock should still be released
	lockPath := filepath.Join(sm.stateDir, ".lock")
	if _, statErr := os.Stat(lockPath); !os.IsNotExist(statErr) {
		t.Error("Lock should be released even after flush error")
	}
}

func TestStateManager_WipeState(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Write checkpoint
	offset := int64(12345)
	sm.stores["test-store"].Offset = &offset
	assert.NoError(t, sm.Checkpoint(context.Background()))

	// Create some state files
	testFile := filepath.Join(sm.stateDir, "test.db")
	assert.NoError(t, os.WriteFile(testFile, []byte("test"), 0644))

	// Verify files exist
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Fatal("Test file should exist before wipe")
	}

	checkpointPath := filepath.Join(sm.stateDir, "task-0.checkpoint")
	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatal("Checkpoint should exist before wipe")
	}

	// Wipe state
	err := sm.WipeState()
	if err != nil {
		t.Fatalf("WipeState failed: %v", err)
	}

	// Verify directory is wiped
	entries, err := os.ReadDir(sm.stateDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("Failed to read directory: %v", err)
	}

	// Directory may be deleted or empty (depending on implementation)
	// Either is acceptable for wipe
	if len(entries) > 0 {
		// Check that only lock file remains (if we still hold lock)
		for _, entry := range entries {
			if entry.Name() != ".lock" {
				t.Errorf("Unexpected file after wipe: %s", entry.Name())
			}
		}
	}
}

func TestStateManager_Multiple_Stores(t *testing.T) {
	tmpDir := t.TempDir()
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store1 := newMockStateStore("store1", true)
	store2 := newMockStateStore("store2", true)
	store3 := newMockStateStore("store3", false) // Non-persistent

	assert.NoError(t, sm.RegisterStore(store1, true, nil, nil))
	assert.NoError(t, sm.RegisterStore(store2, true, nil, nil))
	assert.NoError(t, sm.RegisterStore(store3, true, nil, nil))

	// Set offsets
	offset1 := int64(100)
	offset2 := int64(200)
	offset3 := int64(300)

	sm.stores["store1"].Offset = &offset1
	sm.stores["store2"].Offset = &offset2
	sm.stores["store3"].Offset = &offset3

	// Checkpoint
	err := sm.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Verify checkpoint contains all persistent stores
	checkpoints, err := sm.checkpointFile.Read()
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	// Should have 2 entries (store3 is non-persistent)
	if len(checkpoints) != 2 {
		t.Errorf("Expected 2 checkpoints, got %d", len(checkpoints))
	}

	tp1 := TopicPartition{Topic: "test-app-store1-changelog", Partition: 0}
	tp2 := TopicPartition{Topic: "test-app-store2-changelog", Partition: 0}

	if checkpoints[tp1] != 100 {
		t.Errorf("Expected store1 offset 100, got %d", checkpoints[tp1])
	}
	if checkpoints[tp2] != 200 {
		t.Errorf("Expected store2 offset 200, got %d", checkpoints[tp2])
	}
}

func TestStateManager_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	// Phase 1: Create StateManager and register stores
	sm := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)

	store := newMockStateStore("test-store", true)
	assert.NoError(t, sm.RegisterStore(store, true, nil, nil))

	// Phase 2: Acquire lock
	err := sm.AcquireLock()
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Phase 3: Initialize from checkpoint (none exists)
	err = sm.InitializeOffsetsFromCheckpoint(false)
	if err != nil {
		t.Fatalf("Failed to initialize offsets: %v", err)
	}

	// Phase 4: Process some records (simulated by setting offset)
	offset := int64(12345)
	sm.stores["test-store"].Offset = &offset

	// Phase 5: Flush stores
	err = sm.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Phase 6: Checkpoint
	err = sm.Checkpoint(context.Background())
	if err != nil {
		t.Fatalf("Checkpoint failed: %v", err)
	}

	// Phase 7: Close
	err = sm.Close(context.Background())
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Phase 8: Restart - create new StateManager
	sm2 := NewStateManager("task-0", 0, "test-app", tmpDir, nil, slog.Default(), nil, 0)
	store2 := newMockStateStore("test-store", true)
	assert.NoError(t, sm2.RegisterStore(store2, true, nil, nil))

	// Acquire lock
	err = sm2.AcquireLock()
	if err != nil {
		t.Fatalf("Failed to acquire lock on restart: %v", err)
	}

	// Initialize from checkpoint (should load offset)
	err = sm2.InitializeOffsetsFromCheckpoint(false)
	if err != nil {
		t.Fatalf("Failed to initialize offsets on restart: %v", err)
	}

	// Verify offset was restored
	metadata := sm2.stores["test-store"]
	if metadata.Offset == nil {
		t.Fatal("Expected offset to be restored from checkpoint")
	}
	if *metadata.Offset != 12345 {
		t.Errorf("Expected offset 12345, got %d", *metadata.Offset)
	}

	assert.NoError(t, sm2.Close(context.Background()))
}
