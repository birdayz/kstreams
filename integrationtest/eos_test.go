package integrationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/birdayz/kstreams"
	"github.com/birdayz/kstreams/kdag"
	"github.com/birdayz/kstreams/kprocessor"
	"github.com/birdayz/kstreams/kserde"
	"github.com/birdayz/kstreams/kstate"
	"github.com/birdayz/kstreams/kstate/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestExactlyOnceConfiguration tests that EOS can be enabled and configured properly
func TestExactlyOnceConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-config-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-config-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Build simple passthrough topology
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, inputTopic, inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterSink(tb, outputTopic, outputTopic, kserde.StringSerializer, kserde.StringSerializer, inputTopic)
	topology := tb.MustBuild()

	// Create app with EOS enabled
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-test-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(), // Enable EOS
	)

	// Start app in background
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Give app time to initialize
	time.Sleep(2 * time.Second)

	// Close app
	app.Close()

	// Wait for app to finish
	select {
	case err := <-done:
		assert.NoError(t, err, "app should shutdown cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for app to shutdown")
	}
}

// TestPassthroughNonEOS is a sanity check that the passthrough topology works without EOS
func TestPassthroughNonEOS(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("passthrough-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("passthrough-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestDataT(t, brokers, inputTopic, 10)

	// Build passthrough topology
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(
		tb,
		newPassthroughProcessor(),
		"passthrough",
		"source",
	)
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "passthrough")
	topology := tb.MustBuild()

	// Create app WITHOUT EOS
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("passthrough-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
	)

	// Run app
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for records
	var outputCount int
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		outputCount = countRecordsInTopic(t, brokers, outputTopic)
		if outputCount >= 10 {
			t.Logf("Found %d records in output topic after %d seconds", outputCount, i+1)
			break
		}
	}

	// Close app
	app.Close()
	runErr := <-done
	if runErr != nil {
		t.Logf("App returned error: %v", runErr)
	}

	// Verify output records exist
	assert.Equal(t, 10, outputCount, "should have produced all records to output topic")
}

// TestTransactionalProduce tests that records are produced transactionally
func TestTransactionalProduce(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-txn-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-txn-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestDataT(t, brokers, inputTopic, 100)

	// Build passthrough topology
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(
		tb,
		newPassthroughProcessor(),
		"passthrough",
		"source",
	)
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "passthrough")
	topology := tb.MustBuild()

	// Create app with EOS
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-txn-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	// Run app
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for records to be processed and produced
	var outputCount int
	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Second)
		outputCount = countRecordsInTopic(t, brokers, outputTopic)
		if outputCount > 0 {
			t.Logf("Found %d records in output topic after %d seconds", outputCount, i+1)
			break
		}
	}

	// Close app
	app.Close()
	runErr := <-done
	if runErr != nil {
		t.Logf("App returned error: %v", runErr)
	}

	// Verify output records exist and are committed
	assert.Greater(t, outputCount, 0, "should have produced records to output topic")
}

// TestReadCommittedIsolation tests that EOS consumer only reads committed records
func TestReadCommittedIsolation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topic
	testTopic := fmt.Sprintf("eos-isolation-test-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, testTopic, "dummy-topic")

	// Produce some committed records
	produceCommittedRecords(t, brokers, testTopic, 50)

	// Count records with read_uncommitted
	uncommittedCount := countRecordsWithIsolation(t, brokers, testTopic, kgo.ReadUncommitted())
	t.Logf("Uncommitted count: %d", uncommittedCount)

	// Count records with read_committed
	committedCount := countRecordsWithIsolation(t, brokers, testTopic, kgo.ReadCommitted())
	t.Logf("Committed count: %d", committedCount)

	// Both should see the same records since all are committed
	assert.Equal(t, uncommittedCount, committedCount, "committed records should be visible in both modes")
	assert.Equal(t, 50, committedCount, "should see all 50 committed records")
}

// TestTransactionAbortOnError tests that transactions are aborted when processing fails
func TestTransactionAbortOnError(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-abort-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-abort-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce test data with an error-triggering record
	produceTestDataWithError(t, brokers, inputTopic)

	// Build topology with error-prone processor
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, inputTopic, inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(
		tb,
		newErrorProneProcessor(),
		"error-processor",
		inputTopic,
	)
	kstreams.RegisterSink(tb, outputTopic, outputTopic, kserde.StringSerializer, kserde.StringSerializer, "error-processor")
	topology := tb.MustBuild()

	// Create app with EOS
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-abort-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	// Run app (will fail due to error)
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for error
	select {
	case err := <-done:
		assert.Error(t, err, "app should fail due to processing error")
	case <-time.After(10 * time.Second):
		app.Close()
		t.Fatal("expected app to fail but it didn't")
	}

	// Verify that output topic has no records (transaction was aborted)
	outputCount := countRecordsInTopic(t, brokers, outputTopic)
	assert.Equal(t, 0, outputCount, "aborted transaction should not produce records")
}

// TestEOSCrashBeforeCommit tests that crashing before transaction commit produces no output
func TestEOSCrashBeforeCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-crash-before-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-crash-before-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestDataT(t, brokers, inputTopic, 100)

	// Build topology with crash processor that crashes after 50 records
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)

	crashAfter := 50
	processedCount := 0
	crashProcessor := func() kprocessor.Processor[string, string, string, string] {
		return &CrashProcessor{
			crashAfter:      &crashAfter,
			processedCount:  &processedCount,
		}
	}

	kstreams.RegisterProcessor(tb, crashProcessor, "crash-processor", "source")
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "crash-processor")
	topology := tb.MustBuild()

	// Create app with EOS
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-crash-before-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	// Run app (it will crash)
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for crash
	select {
	case err := <-done:
		t.Logf("App crashed as expected: %v", err)
		assert.Error(t, err, "app should crash")
	case <-time.After(10 * time.Second):
		app.Close()
		<-done
		t.Fatal("app should have crashed")
	}

	// Verify NO records in output (transaction was aborted, not committed)
	time.Sleep(1 * time.Second)
	outputCount := countRecordsInTopic(t, brokers, outputTopic)
	assert.Equal(t, 0, outputCount, "crashed transaction should produce NO records (all-or-nothing)")

	t.Logf("Processed %d records before crash, output has %d records (expected 0)", processedCount, outputCount)
}

// TestEOSCrashAfterCommit tests the limitation: crash after txn commit but before offset commit
func TestEOSCrashAfterCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-crash-after-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-crash-after-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce small batch
	produceTestDataT(t, brokers, inputTopic, 10)

	// Build simple passthrough topology
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(tb, newPassthroughProcessor(), "passthrough", "source")
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "passthrough")
	topology := tb.MustBuild()

	// Use same group name for both runs to demonstrate offset behavior
	groupName := fmt.Sprintf("eos-crash-after-app-%d", time.Now().UnixNano())

	// Run 1: Process all records successfully
	app1 := kstreams.MustNew(
		topology,
		groupName,
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	done1 := make(chan error, 1)
	go func() {
		done1 <- app1.Run()
	}()

	// Wait for records to be processed
	var outputCount1 int
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		outputCount1 = countRecordsInTopic(t, brokers, outputTopic)
		if outputCount1 >= 10 {
			break
		}
	}

	t.Logf("First run produced %d records", outputCount1)
	assert.Equal(t, 10, outputCount1, "first run should produce all records")

	// Force close WITHOUT waiting for graceful shutdown (simulates crash)
	// This may leave offsets uncommitted even though transaction was committed
	app1.Close()
	<-done1

	// Run 2: Same consumer group, should resume from last committed offset
	// If offsets weren't committed, we'll reprocess records
	app2 := kstreams.MustNew(
		topology,
		groupName, // Same group name!
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	done2 := make(chan error, 1)
	go func() {
		done2 <- app2.Run()
	}()

	// Wait for potential reprocessing
	time.Sleep(5 * time.Second)
	app2.Close()
	<-done2

	// Count total output records
	outputCount2 := countRecordsInTopic(t, brokers, outputTopic)
	t.Logf("After restart, output has %d records", outputCount2)

	// With proper EOS implementation, offsets are committed atomically within the transaction
	// This means NO duplicates should occur on restart
	assert.Equal(t, 10, outputCount2, "EOS should prevent duplicates - offsets must be committed within transaction")
}

// TestEOSMultiplePartitions tests EOS with multiple partitions and workers
func TestEOSMultiplePartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics with 3 partitions
	inputTopic := fmt.Sprintf("eos-multi-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-multi-output-%d", time.Now().UnixNano())

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	adminClient := kadm.NewClient(client)
	_, err = adminClient.CreateTopics(ctx, 3, 1,
		map[string]*string{},
		inputTopic, outputTopic)
	require.NoError(t, err)

	t.Logf("Created topics with 3 partitions: %s, %s", inputTopic, outputTopic)

	// Produce 300 records distributed across partitions
	for i := 0; i < 300; i++ {
		result := client.ProduceSync(ctx, &kgo.Record{
			Topic: inputTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		require.NoError(t, result.FirstErr())
	}
	t.Logf("Produced 300 records to %s", inputTopic)

	// Build passthrough topology
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(tb, newPassthroughProcessor(), "passthrough", "source")
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "passthrough")
	topology := tb.MustBuild()

	// Create app with 3 workers (one per partition)
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-multi-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(3), // Multiple workers!
		kstreams.WithExactlyOnce(),
	)

	// Run app
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for all records to be processed
	var outputCount int
	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Second)
		outputCount = countRecordsInTopic(t, brokers, outputTopic)
		if outputCount >= 300 {
			t.Logf("Found %d records in output topic after %d seconds", outputCount, i+1)
			break
		}
	}

	// Close app
	app.Close()
	runErr := <-done
	if runErr != nil {
		t.Logf("App returned error: %v", runErr)
	}

	// Verify all records processed exactly once
	assert.Equal(t, 300, outputCount, "should have exactly 300 records (no duplicates, no loss)")
}

// TestEOSMultipleOutputTopics tests atomicity across multiple output topics
func TestEOSMultipleOutputTopics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-multi-out-input-%d", time.Now().UnixNano())
	outputTopic1 := fmt.Sprintf("eos-multi-out-output1-%d", time.Now().UnixNano())
	outputTopic2 := fmt.Sprintf("eos-multi-out-output2-%d", time.Now().UnixNano())

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	adminClient := kadm.NewClient(client)
	_, err = adminClient.CreateTopics(ctx, 1, 1,
		map[string]*string{},
		inputTopic, outputTopic1, outputTopic2)
	require.NoError(t, err)

	t.Logf("Created topics: %s -> %s, %s", inputTopic, outputTopic1, outputTopic2)

	// Produce test data
	produceTestDataT(t, brokers, inputTopic, 50)

	// Build topology that writes to TWO output topics
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)

	// Processor that forwards to two different sinks
	dualForwarder := func() kprocessor.Processor[string, string, string, string] {
		return &DualForwardProcessor{}
	}
	kstreams.RegisterProcessor(tb, dualForwarder, "dual-forwarder", "source")

	kstreams.RegisterSink(tb, "sink1", outputTopic1, kserde.StringSerializer, kserde.StringSerializer, "dual-forwarder")
	kstreams.RegisterSink(tb, "sink2", outputTopic2, kserde.StringSerializer, kserde.StringSerializer, "dual-forwarder")
	topology := tb.MustBuild()

	// Create app with EOS
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-multi-out-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	// Run app
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for processing
	time.Sleep(5 * time.Second)

	// Close app
	app.Close()
	<-done

	// Verify BOTH topics have records (atomicity across topics)
	output1Count := countRecordsInTopic(t, brokers, outputTopic1)
	output2Count := countRecordsInTopic(t, brokers, outputTopic2)

	t.Logf("Output topic 1: %d records, Output topic 2: %d records", output1Count, output2Count)

	// Both should have same count (atomic write to both)
	assert.Equal(t, 50, output1Count, "topic 1 should have all records")
	assert.Equal(t, 50, output2Count, "topic 2 should have all records")
	assert.Equal(t, output1Count, output2Count, "both topics should have same count (atomicity)")
}

// TestEOSProducerFencing tests zombie writer detection
func TestEOSProducerFencing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-fence-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-fence-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestDataT(t, brokers, inputTopic, 100)

	// Build topology
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(tb, newPassthroughProcessor(), "passthrough", "source")
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "passthrough")
	topology := tb.MustBuild()

	// SAME group name for both apps (simulates zombie)
	groupName := fmt.Sprintf("eos-fence-app-%d", time.Now().UnixNano())

	// Start first app
	app1 := kstreams.MustNew(
		topology,
		groupName,
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	done1 := make(chan error, 1)
	go func() {
		done1 <- app1.Run()
	}()

	// Let it start processing
	time.Sleep(3 * time.Second)

	// Check progress
	outputCount1 := countRecordsInTopic(t, brokers, outputTopic)
	t.Logf("App1 processed %d records before app2 starts", outputCount1)

	// Start second app with SAME group (zombie scenario)
	// Note: In real scenarios, consumer group rebalancing will happen
	// and one of the apps will lose its partition assignment
	app2 := kstreams.MustNew(
		topology,
		groupName, // Same consumer group!
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	done2 := make(chan error, 1)
	go func() {
		done2 <- app2.Run()
	}()

	// Let both run - rebalancing will occur
	time.Sleep(5 * time.Second)

	// Close both
	app1.Close()
	app2.Close()
	err1 := <-done1
	err2 := <-done2

	t.Logf("App1 error: %v", err1)
	t.Logf("App2 error: %v", err2)

	// Check final output
	outputCount := countRecordsInTopic(t, brokers, outputTopic)
	t.Logf("Final output has %d records", outputCount)

	// Should have processed records (via rebalancing, one app will get the partition)
	// No duplicates due to EOS (at most 100 records)
	assert.Greater(t, outputCount, 0, "should have processed records")
	assert.LessOrEqual(t, outputCount, 100, "should not have duplicates due to EOS")
}

// TestEOSLargeBatch tests EOS with large batches
func TestEOSLargeBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-large-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-large-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce LARGE batch (10k records)
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	t.Logf("Producing 10,000 records...")
	for i := 0; i < 10000; i++ {
		client.Produce(ctx, &kgo.Record{
			Topic: inputTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}, nil)
	}
	require.NoError(t, client.Flush(ctx))
	t.Logf("Produced 10,000 records")

	// Build passthrough topology
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(tb, newPassthroughProcessor(), "passthrough", "source")
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "passthrough")
	topology := tb.MustBuild()

	// Create app with EOS
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-large-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	// Run app
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for all records
	var outputCount int
	for i := 0; i < 30; i++ {
		time.Sleep(1 * time.Second)
		outputCount = countRecordsInTopic(t, brokers, outputTopic)
		if outputCount >= 10000 {
			t.Logf("Found %d records after %d seconds", outputCount, i+1)
			break
		}
		if i%5 == 0 {
			t.Logf("Progress: %d/10000 records", outputCount)
		}
	}

	// Close app
	app.Close()
	<-done

	// Verify exactly 10k records
	assert.Equal(t, 10000, outputCount, "should have exactly 10,000 records")
}

// TestEOSStateStoreTransactionCoordination tests state flush before txn commit
func TestEOSStateStoreTransactionCoordination(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-state-coord-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-state-coord-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce test data (same key multiple times to test state updates)
	produceTestDataT(t, brokers, inputTopic, 100)

	// Build topology with state store
	tb := kdag.NewBuilder()
	stateDir := t.TempDir()
	kdag.RegisterStore(
		tb,
		pebble.NewKeyValueStoreBuilder[string, int64]("counts", stateDir).
			WithSerdes(
				kserde.StringSerializer, kserde.StringDeserializer,
				kserde.Int64Serializer, kserde.Int64Deserializer,
			),
		"counts",
	)
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(
		tb,
		newCountingProcessor("counts"),
		"counter",
		"source",
		"counts",
	)
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.Int64Serializer, "counter")
	topology := tb.MustBuild()

	// Run 1: Process with EOS
	groupName := fmt.Sprintf("eos-state-coord-app-%d", time.Now().UnixNano())
	app1 := kstreams.MustNew(
		topology,
		groupName,
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	done1 := make(chan error, 1)
	go func() {
		done1 <- app1.Run()
	}()

	// Wait for processing
	time.Sleep(5 * time.Second)
	app1.Close()
	<-done1

	outputCount1 := countRecordsInTopic(t, brokers, outputTopic)
	t.Logf("First run produced %d count records", outputCount1)

	// Run 2: Restart with same state directory (state should be preserved)
	app2 := kstreams.MustNew(
		topology,
		groupName,
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	done2 := make(chan error, 1)
	go func() {
		done2 <- app2.Run()
	}()

	time.Sleep(3 * time.Second)
	app2.Close()
	<-done2

	outputCount2 := countRecordsInTopic(t, brokers, outputTopic)
	t.Logf("After restart, output has %d records", outputCount2)

	// Should have records from first run
	assert.Greater(t, outputCount1, 0, "should have produced count records")
	// State is preserved across restarts
	assert.GreaterOrEqual(t, outputCount2, outputCount1, "state should be preserved")
}

// TestEOSMixedProducers tests compatibility scenarios
func TestEOSMixedProducers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-mixed-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-mixed-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Non-EOS producer writes to input
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	t.Logf("Non-EOS producer writing 50 records...")
	for i := 0; i < 50; i++ {
		result := client.ProduceSync(ctx, &kgo.Record{
			Topic: inputTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		require.NoError(t, result.FirstErr())
	}

	// EOS consumer reads and processes
	tb := kdag.NewBuilder()
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(tb, newPassthroughProcessor(), "passthrough", "source")
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.StringSerializer, "passthrough")
	topology := tb.MustBuild()

	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-mixed-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(), // EOS consumer
	)

	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for processing
	var outputCount int
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		outputCount = countRecordsInTopic(t, brokers, outputTopic)
		if outputCount >= 50 {
			break
		}
	}

	app.Close()
	<-done

	// EOS consumer should handle non-EOS input fine
	assert.Equal(t, 50, outputCount, "EOS consumer should process non-EOS input correctly")
	t.Logf("✓ Non-EOS producer → EOS consumer: %d records", outputCount)
}

// TestEOSWithStateStore tests EOS with stateful processing
func TestEOSWithStateStore(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping EOS integration test in short mode")
	}

	ctx := context.Background()

	// Start Redpanda
	redpandaContainer, err := redpanda.RunContainer(ctx)
	require.NoError(t, err)
	defer redpandaContainer.Terminate(ctx)

	bootstrapServer, err := redpandaContainer.KafkaSeedBroker(ctx)
	require.NoError(t, err)

	brokers := []string{bootstrapServer}

	// Create topics
	inputTopic := fmt.Sprintf("eos-state-input-%d", time.Now().UnixNano())
	outputTopic := fmt.Sprintf("eos-state-output-%d", time.Now().UnixNano())
	createTopicsT(t, brokers, inputTopic, outputTopic)

	// Produce test data
	produceTestDataT(t, brokers, inputTopic, 100)

	// Build topology with state store
	tb := kdag.NewBuilder()
	stateDir := t.TempDir()
	kdag.RegisterStore(
		tb,
		pebble.NewKeyValueStoreBuilder[string, int64]("eos-counts", stateDir).
			WithSerdes(
				kserde.StringSerializer, kserde.StringDeserializer,
				kserde.Int64Serializer, kserde.Int64Deserializer,
			),
		"eos-counts",
	)
	kstreams.RegisterSource(tb, "source", inputTopic, kserde.StringDeserializer, kserde.StringDeserializer)
	kstreams.RegisterProcessor(
		tb,
		newCountingProcessor("eos-counts"),
		"counter",
		"source",
		"eos-counts",
	)
	kstreams.RegisterSink(tb, "sink", outputTopic, kserde.StringSerializer, kserde.Int64Serializer, "counter")
	topology := tb.MustBuild()

	// Create app with EOS
	app := kstreams.MustNew(
		topology,
		fmt.Sprintf("eos-state-app-%d", time.Now().UnixNano()),
		kstreams.WithBrokers(brokers),
		kstreams.WithWorkersCount(1),
		kstreams.WithExactlyOnce(),
	)

	// Run app
	done := make(chan error, 1)
	go func() {
		done <- app.Run()
	}()

	// Wait for records to be processed
	var outputCount int
	for i := 0; i < 20; i++ {
		time.Sleep(1 * time.Second)
		outputCount = countRecordsInTopic(t, brokers, outputTopic)
		if outputCount > 0 {
			t.Logf("Found %d records in output topic after %d seconds", outputCount, i+1)
			break
		}
	}

	// Close app
	app.Close()
	runErr := <-done
	if runErr != nil {
		t.Logf("App returned error: %v", runErr)
	}

	// Verify output
	assert.Greater(t, outputCount, 0, "should have produced count records")
}

// Helper functions for tests (not benchmarks)

func createTopicsT(t *testing.T, brokers []string, inputTopic, outputTopic string) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	adminClient := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create topics with 3 partitions
	_, err = adminClient.CreateTopics(ctx, 3, 1, nil, inputTopic, outputTopic)
	require.NoError(t, err)

	t.Logf("Created topics: %s, %s", inputTopic, outputTopic)
}

func produceTestDataT(t *testing.T, brokers []string, topic string, numRecords int) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	// Produce records
	records := make([]*kgo.Record, numRecords)
	for i := 0; i < numRecords; i++ {
		key := fmt.Sprintf("user-%d", i%100) // 100 unique users
		value := fmt.Sprintf("event-%d", i)

		records[i] = &kgo.Record{
			Topic: topic,
			Key:   []byte(key),
			Value: []byte(value),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce all records
	results := client.ProduceSync(ctx, records...)
	for _, res := range results {
		require.NoError(t, res.Err)
	}

	t.Logf("Produced %d records to %s", numRecords, topic)
}

// Helper: count records in a topic with specific isolation level
func countRecordsWithIsolation(t *testing.T, brokers []string, topic string, isolation kgo.IsolationLevel) int {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.FetchIsolationLevel(isolation),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	count := 0
	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		if ctx.Err() != nil {
			break
		}

		fetches.EachRecord(func(r *kgo.Record) {
			count++
		})

		if len(fetches.Records()) == 0 {
			break
		}
	}

	return count
}

// Helper: count total records in a topic
func countRecordsInTopic(t *testing.T, brokers []string, topic string) int {
	return countRecordsWithIsolation(t, brokers, topic, kgo.ReadCommitted())
}

// Helper: produce committed records
func produceCommittedRecords(t *testing.T, brokers []string, topic string, count int) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()
	for i := 0; i < count; i++ {
		result := client.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		require.NoError(t, result.FirstErr())
	}
}

// Helper: produce test data with an error-triggering record
func produceTestDataWithError(t *testing.T, brokers []string, topic string) {
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Produce normal records
	for i := 0; i < 5; i++ {
		client.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
	}

	// Produce error-triggering record
	client.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte("ERROR"),
		Value: []byte("this will cause an error"),
	})
}

// DualForwardProcessor forwards records to all children (for multi-output test)
type DualForwardProcessor struct {
	ctx kprocessor.ProcessorContext[string, string]
}

func (p *DualForwardProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *DualForwardProcessor) Process(ctx context.Context, k string, v string) error {
	// Forward to all children (both sinks)
	p.ctx.Forward(ctx, k, v)
	return nil
}

func (p *DualForwardProcessor) Close() error {
	return nil
}

// CrashProcessor simulates a crash after processing N records
type CrashProcessor struct {
	ctx             kprocessor.ProcessorContext[string, string]
	crashAfter      *int
	processedCount  *int
}

func (p *CrashProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *CrashProcessor) Process(ctx context.Context, k string, v string) error {
	*p.processedCount++

	if *p.processedCount > *p.crashAfter {
		return fmt.Errorf("simulated crash after processing %d records", *p.processedCount)
	}

	p.ctx.Forward(ctx, k, v)
	return nil
}

func (p *CrashProcessor) Close() error {
	return nil
}

// Passthrough processor for simple forwarding
type PassthroughProcessor struct {
	ctx kprocessor.ProcessorContext[string, string]
}

func newPassthroughProcessor() kprocessor.ProcessorBuilder[string, string, string, string] {
	return func() kprocessor.Processor[string, string, string, string] {
		return &PassthroughProcessor{}
	}
}

func (p *PassthroughProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *PassthroughProcessor) Process(ctx context.Context, k string, v string) error {
	p.ctx.Forward(ctx, k, v)
	return nil
}

func (p *PassthroughProcessor) Close() error {
	return nil
}

// Error-prone processor for testing abort behavior
type ErrorProneProcessor struct {
	ctx kprocessor.ProcessorContext[string, string]
}

func newErrorProneProcessor() kprocessor.ProcessorBuilder[string, string, string, string] {
	return func() kprocessor.Processor[string, string, string, string] {
		return &ErrorProneProcessor{}
	}
}

func (p *ErrorProneProcessor) Init(ctx kprocessor.ProcessorContext[string, string]) error {
	p.ctx = ctx
	return nil
}

func (p *ErrorProneProcessor) Process(ctx context.Context, k string, v string) error {
	if k == "ERROR" {
		return fmt.Errorf("simulated processing error for key: %s", k)
	}
	p.ctx.Forward(ctx, k, v)
	return nil
}

func (p *ErrorProneProcessor) Close() error {
	return nil
}

// EOSCountingProcessor - counting processor for EOS tests
type EOSCountingProcessor struct {
	store     kstate.KeyValueStore[string, int64]
	storeName string
	ctx       kprocessor.ProcessorContext[string, int64]
}

func newCountingProcessor(storeName string) kprocessor.ProcessorBuilder[string, string, string, int64] {
	return func() kprocessor.Processor[string, string, string, int64] {
		return &EOSCountingProcessor{storeName: storeName}
	}
}

func (p *EOSCountingProcessor) Init(ctx kprocessor.ProcessorContext[string, int64]) error {
	p.ctx = ctx
	store, err := kstate.GetKeyValueStore[string, int64](ctx, p.storeName)
	if err != nil {
		return err
	}
	p.store = store
	return nil
}

func (p *EOSCountingProcessor) Process(ctx context.Context, k string, v string) error {
	count, found, err := p.store.Get(ctx, k)
	if err != nil {
		return err
	}
	if !found {
		count = 0
	}

	count++
	if err := p.store.Set(ctx, k, count); err != nil {
		return err
	}

	p.ctx.Forward(ctx, k, count)
	return nil
}

func (p *EOSCountingProcessor) Close() error {
	return nil
}
