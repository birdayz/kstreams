package checkpoint

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// TopicPartition represents a Kafka topic-partition pair
// Used for tracking changelog offsets in checkpoint files
type TopicPartition struct {
	Topic     string
	Partition int32
}

// CheckpointFile manages persistent offset storage for state store changelogs
// Matches Kafka Streams' OffsetCheckpoint file format EXACTLY
//
// File format (text-based, matching Kafka Streams):
// Line 1: Version number (currently 0)
// Line 2: Number of entries
// Line 3+: Space-separated "<topic> <partition> <offset>"
//
// Example:
//   0
//   2
//   my-app-store-changelog 0 12345
//   my-app-store-changelog 1 67890
//
// Sentinel values:
//   - -4 (OFFSET_UNKNOWN): Offset is unknown, will restore from beginning
//   - Missing entry: No checkpoint exists, restore from beginning
type CheckpointFile struct {
	Path string     // Exported for access from statemgr
	lock sync.Mutex // Thread-safe read/write operations
}

const (
	// OFFSET_UNKNOWN indicates that the offset is unknown
	// Matches Kafka Streams' OffsetCheckpoint.OFFSET_UNKNOWN = -4L
	//
	// Why -4? From Kafka Streams:
	// - -1 may be taken by some producer errors
	// - -2 in subscription means state is used by an active task
	// - -3 is also used in subscription
	// - -4 is chosen to avoid conflicts
	OFFSET_UNKNOWN = int64(-4)

	// VERSION is the checkpoint file format version
	// Matches Kafka Streams' OffsetCheckpoint.VERSION = 0
	VERSION = 0
)

// NewCheckpointFile creates a new checkpoint file manager
// path is the full file path (e.g., "/tmp/state/0_0.checkpoint")
func NewCheckpointFile(path string) *CheckpointFile {
	return &CheckpointFile{Path: path}
}

// Read loads checkpoint offsets from disk
// Returns empty map if file doesn't exist (not an error)
// Returns error only if file exists but can't be read/parsed
//
// Matches Kafka Streams' OffsetCheckpoint.read() behavior
func (c *CheckpointFile) Read() (map[TopicPartition]int64, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	file, err := os.Open(c.Path)
	if err != nil {
		if os.IsNotExist(err) {
			// No checkpoint file = empty checkpoints (not an error)
			return make(map[TopicPartition]int64), nil
		}
		return nil, fmt.Errorf("open checkpoint file: %w", err)
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	// Line 1: Read version
	if !scanner.Scan() {
		return nil, fmt.Errorf("checkpoint file is empty")
	}
	lineNum++
	versionStr := strings.TrimSpace(scanner.Text())
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return nil, fmt.Errorf("line %d: invalid version: %w", lineNum, err)
	}
	if version != VERSION {
		return nil, fmt.Errorf("unknown checkpoint version: %d (expected %d)", version, VERSION)
	}

	// Line 2: Read entry count
	if !scanner.Scan() {
		return nil, fmt.Errorf("missing entry count on line 2")
	}
	lineNum++
	countStr := strings.TrimSpace(scanner.Text())
	expectedCount, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, fmt.Errorf("line %d: invalid entry count: %w", lineNum, err)
	}

	// Lines 3+: Read entries
	checkpoints := make(map[TopicPartition]int64)
	actualCount := 0
	skippedCount := 0 // Track invalid offsets we skip

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			// CRITICAL: Don't skip empty lines - this diverges from Kafka Streams
			// Empty lines should cause parse errors
			return nil, fmt.Errorf("line %d: unexpected empty line in checkpoint file", lineNum)
		}

		parts := strings.Fields(line)
		if len(parts) != 3 {
			return nil, fmt.Errorf("line %d: expected 3 fields (topic partition offset), got %d: %s",
				lineNum, len(parts), line)
		}

		topic := parts[0]
		partition, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid partition: %w", lineNum, err)
		}

		offset, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid offset: %w", lineNum, err)
		}

		// Validate offset (must be >= 0 or OFFSET_UNKNOWN)
		if !isValidOffset(offset) {
			// Skip invalid offsets (matches Kafka Streams behavior)
			// The count validation at the end will catch truly corrupted files
			skippedCount++
			continue
		}

		tp := TopicPartition{
			Topic:     topic,
			Partition: int32(partition),
		}
		checkpoints[tp] = offset
		actualCount++
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading checkpoint file: %w", err)
	}

	// Validate entry count matches (accounting for skipped invalid offsets)
	// CRITICAL: Throw error instead of warning (matches Kafka Streams)
	// Corrupted checkpoint files should fail fast, not be silently accepted
	if actualCount != (expectedCount - skippedCount) {
		return nil, fmt.Errorf("expected %d entries but found only %d (skipped %d invalid)",
			expectedCount, actualCount, skippedCount)
	}

	return checkpoints, nil
}

// Write persists checkpoint offsets to disk atomically
// Uses write-to-temp-then-rename + fsync pattern to ensure atomicity
// Matches Kafka Streams' OffsetCheckpoint.write() behavior
func (c *CheckpointFile) Write(checkpoints map[TopicPartition]int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// If checkpoints are empty, delete the file instead of writing empty content
	// This matches Kafka Streams behavior
	if len(checkpoints) == 0 {
		return c.deleteWithoutLock()
	}

	// Ensure directory exists
	dir := filepath.Dir(c.Path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}

	// Atomic write: write to temp file, fsync, then rename
	// This prevents corruption if process crashes during write
	tmpPath := c.Path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}

	writer := bufio.NewWriter(file)

	// Line 1: Write version
	if _, err := fmt.Fprintf(writer, "%d\n", VERSION); err != nil {
		_ = file.Close() // Intentionally ignore close error on write failure
		return fmt.Errorf("write version: %w", err)
	}

	// Line 2: Write entry count
	validCount := 0
	for _, offset := range checkpoints {
		if isValidOffset(offset) {
			validCount++
		}
	}
	if _, err := fmt.Fprintf(writer, "%d\n", validCount); err != nil {
		_ = file.Close() // Intentionally ignore close error on write failure
		return fmt.Errorf("write count: %w", err)
	}

	// Lines 3+: Write entries (topic partition offset)
	for tp, offset := range checkpoints {
		// Validate offset before writing
		// CRITICAL: Throw error for invalid offsets (matches Kafka Streams)
		// Invalid offsets indicate programmer error and should fail fast
		if !isValidOffset(offset) {
			_ = file.Close() // Intentionally ignore close error on validation failure
			_ = os.Remove(tmpPath) // Clean up temp file
			return fmt.Errorf("invalid offset %d for %s-%d: offsets must be >= 0 or OFFSET_UNKNOWN (-4)",
				offset, tp.Topic, tp.Partition)
		}

		if _, err := fmt.Fprintf(writer, "%s %d %d\n", tp.Topic, tp.Partition, offset); err != nil {
			_ = file.Close() // Intentionally ignore close error on write failure
			return fmt.Errorf("write entry: %w", err)
		}
	}

	// Flush buffered writer
	if err := writer.Flush(); err != nil {
		_ = file.Close() // Intentionally ignore close error on flush failure
		return fmt.Errorf("flush buffer: %w", err)
	}

	// Sync to disk (fsync) to ensure durability before rename
	// This matches Kafka Streams' FileDescriptor.sync()
	if err := file.Sync(); err != nil {
		_ = file.Close() // Intentionally ignore close error on sync failure
		return fmt.Errorf("sync checkpoint: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp checkpoint: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, c.Path); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	// CRITICAL: Fsync parent directory to ensure rename is durable
	// Without this, the rename may be lost on crash (directory metadata not flushed)
	// Matches Kafka Streams' Utils.atomicMoveWithFallback() behavior
	// Skip on Windows and ZOS (like Kafka Streams)
	// Note: dir was already computed at line 189
	if runtime.GOOS != "windows" && runtime.GOOS != "zos" {
		dirFile, err := os.Open(dir)
		if err != nil {
			return fmt.Errorf("open directory for fsync: %w", err)
		}
		defer func() { _ = dirFile.Close() }()

		if err := dirFile.Sync(); err != nil {
			return fmt.Errorf("fsync directory: %w", err)
		}
	}

	return nil
}

// Delete removes the checkpoint file
// Used when wiping state (e.g., task reset)
func (c *CheckpointFile) Delete() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.deleteWithoutLock()
}

// deleteWithoutLock removes the checkpoint file without acquiring lock
// Internal helper for Write() when checkpoints are empty
func (c *CheckpointFile) deleteWithoutLock() error {
	if err := os.Remove(c.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete checkpoint: %w", err)
	}
	return nil
}

// isValidOffset validates an offset value
// Offsets must be >= 0 or exactly OFFSET_UNKNOWN (-4)
// Matches Kafka Streams' OffsetCheckpoint.isValid()
func isValidOffset(offset int64) bool {
	return offset >= 0 || offset == OFFSET_UNKNOWN
}

// Offset represents a Kafka offset with explicit unknown state.
// Prefer this type over raw int64 to avoid confusion with magic values.
//
// State machine:
//   - Unknown: Initial state, restored from beginning
//   - Known(value): Has a valid offset value (>= 0)
type Offset struct {
	value   int64
	known   bool
}

// UnknownOffset returns an offset in the unknown state.
// Unknown offsets cause restoration from the beginning of the changelog.
func UnknownOffset() Offset {
	return Offset{known: false}
}

// KnownOffset creates an offset with a known value.
// Panics if value is negative.
func KnownOffset(value int64) Offset {
	if value < 0 {
		panic(fmt.Sprintf("offset must be >= 0, got %d", value))
	}
	return Offset{value: value, known: true}
}

// OffsetFromCheckpoint converts a checkpoint file value to an Offset.
// OFFSET_UNKNOWN (-4) becomes Unknown, other valid values become Known.
func OffsetFromCheckpoint(checkpoint int64) Offset {
	if checkpoint == OFFSET_UNKNOWN {
		return UnknownOffset()
	}
	if checkpoint < 0 {
		// Invalid value - treat as unknown
		return UnknownOffset()
	}
	return KnownOffset(checkpoint)
}

// IsKnown returns true if the offset has a known value.
func (o Offset) IsKnown() bool {
	return o.known
}

// Value returns the offset value.
// Panics if the offset is unknown - check IsKnown() first.
func (o Offset) Value() int64 {
	if !o.known {
		panic("cannot get value of unknown offset")
	}
	return o.value
}

// ValueOr returns the offset value, or the default if unknown.
func (o Offset) ValueOr(defaultValue int64) int64 {
	if !o.known {
		return defaultValue
	}
	return o.value
}

// ToCheckpoint converts the Offset to checkpoint file format.
// Unknown offsets are written as OFFSET_UNKNOWN (-4).
func (o Offset) ToCheckpoint() int64 {
	if !o.known {
		return OFFSET_UNKNOWN
	}
	return o.value
}

// NextFetchOffset returns the next offset to fetch from Kafka.
// For unknown offsets, returns 0 (fetch from beginning).
// For known offsets, returns value + 1 (next offset after current).
func (o Offset) NextFetchOffset() int64 {
	if !o.known {
		return 0
	}
	return o.value + 1
}

// String returns a human-readable representation.
func (o Offset) String() string {
	if !o.known {
		return "unknown"
	}
	return fmt.Sprintf("%d", o.value)
}

