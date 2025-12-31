package kstreams

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// DirectoryLock provides exclusive access to a state directory
// Matches Kafka Streams' StateDirectory lock file behavior
//
// The lock file prevents multiple processes/tasks from accessing the same state directory
// This is critical for data integrity - concurrent access could corrupt state stores
//
// Lock file lifecycle:
//  1. Lock(): Create .lock file and acquire exclusive lock
//  2. Unlock(): Release lock and remove .lock file
type DirectoryLock struct {
	lockFilePath string
	lockFile     *os.File
}

// NewDirectoryLock creates a new directory lock
// lockDir is the directory to lock (e.g., "/tmp/state")
func NewDirectoryLock(lockDir string) *DirectoryLock {
	lockFilePath := filepath.Join(lockDir, ".lock")
	return &DirectoryLock{
		lockFilePath: lockFilePath,
	}
}

// Lock acquires an exclusive lock on the directory
// Returns error if:
//   - Lock is already held by this instance (double lock)
//   - Directory doesn't exist or can't be created
//   - Lock is already held by another process
//   - System doesn't support file locking
//
// The lock is implemented using flock(2) on Unix systems
// This provides advisory locking - processes must cooperate
func (l *DirectoryLock) Lock() error {
	// CRITICAL: Prevent double lock to avoid resource leak
	// Matches Kafka Streams' behavior where lock() checks if already locked
	if l.lockFile != nil {
		return fmt.Errorf("lock already held by this instance")
	}

	// Ensure directory exists
	dir := filepath.Dir(l.lockFilePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create lock directory: %w", err)
	}

	// Open or create lock file
	// O_CREATE: Create if doesn't exist
	// O_RDWR: Need read/write for flock
	file, err := os.OpenFile(l.lockFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}

	// Try to acquire exclusive lock (non-blocking)
	// LOCK_EX: Exclusive lock
	// LOCK_NB: Non-blocking (fail immediately if locked)
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return fmt.Errorf("acquire lock (is another instance running?): %w", err)
	}

	l.lockFile = file
	return nil
}

// Unlock releases the lock and removes the lock file
// Returns error only if unlock fails (not if file removal fails)
func (l *DirectoryLock) Unlock() error {
	if l.lockFile == nil {
		return nil // Not locked
	}

	// Save file reference and clear it BEFORE operations
	// CRITICAL: This prevents race conditions where IsLocked() returns true
	// while the file is being closed. Setting to nil first ensures consistent state.
	file := l.lockFile
	l.lockFile = nil

	// Release flock
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); err != nil {
		// CRITICAL: Don't restore l.lockFile on error
		// The file is unusable, so keep it nil to reflect correct state
		file.Close()
		return fmt.Errorf("release lock: %w", err)
	}

	// Close file
	if err := file.Close(); err != nil {
		return fmt.Errorf("close lock file: %w", err)
	}

	// Best-effort removal of lock file
	// Don't fail if removal fails (file might be in use, or permissions changed)
	if err := os.Remove(l.lockFilePath); err != nil && !os.IsNotExist(err) {
		// Log warning but don't return error
		// Lock is already released, file removal is just cleanup
		fmt.Fprintf(os.Stderr, "WARNING: Failed to remove lock file %s: %v\n", l.lockFilePath, err)
	}

	return nil
}

// IsLocked checks if the lock is currently held by this instance
func (l *DirectoryLock) IsLocked() bool {
	return l.lockFile != nil
}
