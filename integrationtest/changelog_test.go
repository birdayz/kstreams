package integrationtest

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/serde"
	"github.com/birdayz/kstreams/stores/pebble"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestChangelogBasic tests basic changelog functionality:
// 1. Create topology with state store (changelog enabled)
// 2. Process records (writes to store + changelog)
// 3. Stop app
// 4. Verify changelog topic contains records
// 5. Delete state files
// 6. Start app again
// 7. Verify state was restored from changelog
func TestChangelogBasic(t *testing.T) {
	t.Skip("TODO: Re-enable after interface migration is complete")

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	// Unique topic and group for this test
	testID := fmt.Sprintf("changelog-basic-%d", time.Now().Unix())
	inputTopic := testID + "-input"
	outputTopic := testID + "-output"
	groupID := testID + "-group"
	stateDir := filepath.Join(os.TempDir(), "kstreams-test-"+testID)

	// Cleanup
	defer os.RemoveAll(stateDir)

	// Create admin client to setup topics
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	assert.NoError(t, err)
	defer adminClient.Close()

	admin := kadm.NewClient(adminClient)

	// Create input and output topics
	_, err = admin.CreateTopics(context.Background(), 1, 1, nil, inputTopic, outputTopic)
	assert.NoError(t, err)

	// Cleanup topics on test end
	defer func() {
		_, _ = admin.DeleteTopics(context.Background(), inputTopic, outputTopic)
		// Also delete changelog topic
		changelogTopic := groupID + "-counts-changelog"
		_, _ = admin.DeleteTopics(context.Background(), changelogTopic)
	}()

	t.Log("Step 1: Build topology with state store (changelog enabled)")

	// Build topology
	builder := kstreams.NewTopologyBuilder()

	// Register source
	kstreams.MustRegisterSource(builder, "source", inputTopic,
		serde.StringDeserializer{},
		serde.StringDeserializer{},
	)

	// Register store with changelog enabled (default behavior for Pebble stores)
	// Note: In new architecture, this would be done via StoreBuilder
	// For now, we'll use a placeholder that would be replaced
	// TODO: Implement proper store registration with new StoreBuilder interface

	t.Skip("Store registration needs refactoring for new StoreBuilder interface")
}

// TestChangelogRestore tests that state is properly restored from changelog after restart
func TestChangelogRestore(t *testing.T) {
	t.Skip("TODO: Implement after basic changelog test works")
}

// TestChangelogTombstones tests that deletions are logged as tombstones
func TestChangelogTombstones(t *testing.T) {
	t.Skip("TODO: Implement after basic changelog test works")
}

// TestChangelogCompaction tests that changelog topics work with log compaction
func TestChangelogCompaction(t *testing.T) {
	t.Skip("TODO: Implement - verify changelog topics are created with cleanup.policy=compact")
}
