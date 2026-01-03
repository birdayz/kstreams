package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointFile_ReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Write checkpoints
	offsets := map[TopicPartition]int64{
		{Topic: "topic1", Partition: 0}: 100,
		{Topic: "topic2", Partition: 1}: 200,
		{Topic: "topic3", Partition: 2}: 300,
	}

	if err := checkpoint.Write(offsets); err != nil {
		t.Fatalf("Failed to write checkpoint: %v", err)
	}

	// Read checkpoints
	readOffsets, err := checkpoint.Read()
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	// Verify
	if len(readOffsets) != len(offsets) {
		t.Errorf("Expected %d offsets, got %d", len(offsets), len(readOffsets))
	}

	for tp, offset := range offsets {
		readOffset, ok := readOffsets[tp]
		if !ok {
			t.Errorf("Missing offset for %v", tp)
		}
		if readOffset != offset {
			t.Errorf("Expected offset %d for %v, got %d", offset, tp, readOffset)
		}
	}
}

func TestCheckpointFile_EmptyCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Write empty checkpoints (should delete file)
	emptyOffsets := map[TopicPartition]int64{}

	if err := checkpoint.Write(emptyOffsets); err != nil {
		t.Fatalf("Failed to write empty checkpoint: %v", err)
	}

	// File should not exist
	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Errorf("Expected checkpoint file to be deleted for empty offsets")
	}

	// Reading should return empty map (not error)
	readOffsets, err := checkpoint.Read()
	if err != nil {
		t.Fatalf("Failed to read non-existent checkpoint: %v", err)
	}

	if len(readOffsets) != 0 {
		t.Errorf("Expected empty offsets, got %d", len(readOffsets))
	}
}

func TestCheckpointFile_OffsetUnknown(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Write checkpoint with OFFSET_UNKNOWN (-4)
	offsets := map[TopicPartition]int64{
		{Topic: "topic1", Partition: 0}: OFFSET_UNKNOWN,
		{Topic: "topic2", Partition: 1}: 100,
	}

	if err := checkpoint.Write(offsets); err != nil {
		t.Fatalf("Failed to write checkpoint with OFFSET_UNKNOWN: %v", err)
	}

	// Read and verify
	readOffsets, err := checkpoint.Read()
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	if readOffsets[TopicPartition{Topic: "topic1", Partition: 0}] != OFFSET_UNKNOWN {
		t.Errorf("Expected OFFSET_UNKNOWN (-4), got %d", readOffsets[TopicPartition{Topic: "topic1", Partition: 0}])
	}
}

func TestCheckpointFile_InvalidOffsetWrite(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Try to write invalid offset (negative but not OFFSET_UNKNOWN)
	offsets := map[TopicPartition]int64{
		{Topic: "topic1", Partition: 0}: -1, // Invalid
	}

	err := checkpoint.Write(offsets)
	if err == nil {
		t.Fatalf("Expected error writing invalid offset, got nil")
	}

	// Should not create checkpoint file on error
	if _, statErr := os.Stat(checkpointPath); !os.IsNotExist(statErr) {
		t.Errorf("Checkpoint file should not exist after failed write")
	}
}

func TestCheckpointFile_InvalidOffsetRead(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")

	// Manually create checkpoint with invalid offset
	content := `0
1
topic1 0 -1
`
	if err := os.WriteFile(checkpointPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	checkpoint := NewCheckpointFile(checkpointPath)
	readOffsets, err := checkpoint.Read()

	// Should succeed but skip invalid offset
	if err != nil {
		t.Fatalf("Expected successful read with skipped invalid offset, got error: %v", err)
	}

	// Invalid offset should be skipped
	if len(readOffsets) != 0 {
		t.Errorf("Expected 0 offsets (invalid skipped), got %d", len(readOffsets))
	}
}

func TestCheckpointFile_CorruptedFile(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")

	tests := []struct {
		name    string
		content string
	}{
		{"empty file", ""},
		{"missing version", ""},
		{"invalid version", "abc\n2\n"},
		{"missing count", "0\n"},
		{"invalid count", "0\nabc\n"},
		{"wrong field count", "0\n1\ntopic1 0\n"},
		{"count mismatch", "0\n3\ntopic1 0 100\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.WriteFile(checkpointPath, []byte(tt.content), 0644); err != nil {
				t.Fatalf("Failed to write test file: %v", err)
			}

			checkpoint := NewCheckpointFile(checkpointPath)
			_, err := checkpoint.Read()
			if err == nil {
				t.Errorf("Expected error reading corrupted checkpoint, got nil")
			}
		})
	}
}

func TestCheckpointFile_AtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Write initial checkpoint
	offsets1 := map[TopicPartition]int64{
		{Topic: "topic1", Partition: 0}: 100,
	}
	if err := checkpoint.Write(offsets1); err != nil {
		t.Fatalf("Failed to write initial checkpoint: %v", err)
	}

	// Overwrite with new checkpoint
	offsets2 := map[TopicPartition]int64{
		{Topic: "topic1", Partition: 0}: 200,
		{Topic: "topic2", Partition: 1}: 300,
	}
	if err := checkpoint.Write(offsets2); err != nil {
		t.Fatalf("Failed to overwrite checkpoint: %v", err)
	}

	// Read and verify new values
	readOffsets, err := checkpoint.Read()
	if err != nil {
		t.Fatalf("Failed to read checkpoint: %v", err)
	}

	if len(readOffsets) != 2 {
		t.Errorf("Expected 2 offsets, got %d", len(readOffsets))
	}

	if readOffsets[TopicPartition{Topic: "topic1", Partition: 0}] != 200 {
		t.Errorf("Expected offset 200, got %d", readOffsets[TopicPartition{Topic: "topic1", Partition: 0}])
	}

	// No .tmp file should remain
	tmpPath := checkpointPath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("Temporary file should not exist: %s", tmpPath)
	}
}

func TestCheckpointFile_Delete(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Write checkpoint
	offsets := map[TopicPartition]int64{
		{Topic: "topic1", Partition: 0}: 100,
	}
	if err := checkpoint.Write(offsets); err != nil {
		t.Fatalf("Failed to write checkpoint: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		t.Fatalf("Checkpoint file should exist")
	}

	// Delete
	if err := checkpoint.Delete(); err != nil {
		t.Fatalf("Failed to delete checkpoint: %v", err)
	}

	// Verify file deleted
	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Errorf("Checkpoint file should be deleted")
	}

	// Delete again should not error
	if err := checkpoint.Delete(); err != nil {
		t.Errorf("Second delete should not error: %v", err)
	}
}

func TestCheckpointFile_ConcurrentReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Write from one goroutine
	done := make(chan error, 1)
	go func() {
		offsets := map[TopicPartition]int64{
			{Topic: "topic1", Partition: 0}: 100,
		}
		done <- checkpoint.Write(offsets)
	}()

	// Read from another goroutine (should be safe due to mutex)
	go func() {
		_, _ = checkpoint.Read()
	}()

	// Wait for write to complete
	if err := <-done; err != nil {
		t.Errorf("Concurrent write failed: %v", err)
	}
}

func TestCheckpointFile_LargeCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	// Create checkpoint with many partitions
	offsets := make(map[TopicPartition]int64)
	for i := 0; i < 1000; i++ {
		offsets[TopicPartition{Topic: "topic", Partition: int32(i)}] = int64(i * 100)
	}

	if err := checkpoint.Write(offsets); err != nil {
		t.Fatalf("Failed to write large checkpoint: %v", err)
	}

	// Read and verify
	readOffsets, err := checkpoint.Read()
	if err != nil {
		t.Fatalf("Failed to read large checkpoint: %v", err)
	}

	if len(readOffsets) != 1000 {
		t.Errorf("Expected 1000 offsets, got %d", len(readOffsets))
	}
}

func TestCheckpointFile_FilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	checkpointPath := filepath.Join(tmpDir, "test.checkpoint")
	checkpoint := NewCheckpointFile(checkpointPath)

	offsets := map[TopicPartition]int64{
		{Topic: "topic1", Partition: 0}: 100,
	}
	if err := checkpoint.Write(offsets); err != nil {
		t.Fatalf("Failed to write checkpoint: %v", err)
	}

	// Check file permissions
	info, err := os.Stat(checkpointPath)
	if err != nil {
		t.Fatalf("Failed to stat checkpoint file: %v", err)
	}

	// File should be readable/writable by owner
	mode := info.Mode()
	if mode&0600 != 0600 {
		t.Errorf("Checkpoint file should have at least rw------- permissions, got %v", mode)
	}
}
