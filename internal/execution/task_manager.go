package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"slices"

	"github.com/birdayz/kstreams/internal/coordination"
	"github.com/birdayz/kstreams/kdag"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type TaskManager struct {
	tasks []*Task

	client *kgo.Client
	log    *slog.Logger

	topology *kdag.DAG

	pgs []*coordination.PartitionGroup

	// State management configuration
	appID    string
	stateDir string
}

type pgPartitions struct {
	partitionGroup *coordination.PartitionGroup
	partitions     []int32
}

func (t *TaskManager) matchingPGs(assignedOrRevoked map[string][]int32) ([]*coordination.PartitionGroup, error) {
	var matchingPGs []*coordination.PartitionGroup

	// For each pg, check if its required topics are there
	for _, pg := range t.pgs {
		matching := 0
		for _, pgTopic := range pg.SourceTopics {
			if _, ok := assignedOrRevoked[pgTopic]; ok {
				matching++
			}
		}

		if matching == 0 {
			// No TopicPartitions assigned for this PG. that's okay
			continue
		}

		if matching < len(pg.SourceTopics) {
			return nil, fmt.Errorf("source topics missing for partition group")
		}

		// All source topics of the TP found => Great!
		matchingPGs = append(matchingPGs, pg)
	}

	return matchingPGs, nil
}

func (t *TaskManager) findPGs(assignedOrRevoked map[string][]int32) ([]*pgPartitions, error) {
	matchingPGs, err := t.matchingPGs(assignedOrRevoked)
	if err != nil {
		return nil, err
	}

	for _, v := range assignedOrRevoked {
		slices.Sort(v)
	}

	var res []*pgPartitions
	for _, pg := range matchingPGs {
		var partitions []int32
		for _, topic := range pg.SourceTopics {
			if partitions != nil {
				assignedPartitions := assignedOrRevoked[topic]
				if !slices.Equal(partitions, assignedPartitions) {
					return nil, fmt.Errorf("partitions not co-assigned. got %v and %v", partitions, assignedPartitions)
				}
			} else {
				partitions = assignedOrRevoked[topic]
			}
		}

		res = append(res, &pgPartitions{
			partitionGroup: pg,
			partitions:     partitions,
		})
	}

	return res, nil

}

// Assigned handles topic-partition assignment: it creates tasks as needed.
func (t *TaskManager) Assigned(assigned map[string][]int32) error {
	matchingPGs, err := t.findPGs(assigned)
	if err != nil {
		return err
	}

	// Note: If ProcessorNames or partitions are nil, there may be a bug in parent-child relationships.
	// There should be only 1 partition group per co-partitioned topic set.
	// TODO: Guarantee each store is initialized only once.

	for _, pg := range matchingPGs {
		for _, partition := range pg.partitions {
			task, err := BuildTaskFromGraph(t.topology.GetGraph(), pg.partitionGroup.SourceTopics, partition, t.client, t.appID, t.stateDir)
			if err != nil {
				return fmt.Errorf("failed to create task: %w", err)
			}
			if err := task.Init(); err != nil {
				// CRITICAL: Clean up resources allocated by BuildTaskFromGraph
				// (state manager, stores, directory lock, etc.)
				_ = task.Close(context.Background())
				return fmt.Errorf("failed to init task: %w", err)
			}

			t.tasks = append(t.tasks, task)
		}

	}

	return nil
}

// Revoked closes and removes tasks as dictated per revoked map.
func (t *TaskManager) Revoked(revoked map[string][]int32) error {
	matchingPGs, err := t.findPGs(revoked)
	if err != nil {
		return err
	}

	// Use timeout context for task close during revocation
	// Kafka rebalance has a session timeout, so we need to complete within that window
	closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, pg := range matchingPGs {
		for _, partition := range pg.partitions {

			// Find task
			found := false
			for i, task := range t.tasks {
				if slices.Equal(task.topics, pg.partitionGroup.SourceTopics) && task.partition == partition {
					found = true
					if err := task.Close(closeCtx); err != nil {
						return err
					}
					t.tasks = slices.Delete(t.tasks, i, i+1)
				}
			}
			if !found {
				return errors.New("could not find task to close/revoke")
			}
		}

	}

	return nil
}

// GetCommittableOffsets returns all committable offsets from all tasks
// without actually committing them. Used for transactional commits.
func (t *TaskManager) GetCommittableOffsets() map[string]map[int32]kgo.EpochOffset {
	data := map[string]map[int32]kgo.EpochOffset{}

	for _, task := range t.tasks {
		commit := task.GetOffsetsToCommit()
		for topic, offset := range commit {
			if _, ok := data[topic]; !ok {
				data[topic] = make(map[int32]kgo.EpochOffset)
			}
			data[topic][task.partition] = kgo.EpochOffset{Epoch: offset.Epoch, Offset: offset.Offset}
		}
	}

	return data
}

// Commit triggers a commit. This flushes all tasks' stores, and then
// performs a commit of all tasks' processed records.
// CRITICAL: After successful offset commit, checkpoints state to prevent data loss on crash
// Matches Kafka Streams' TaskManager.commit() -> task.postCommit() -> checkpoint()
func (t *TaskManager) Commit(ctx context.Context) error {
	// Phase 1: Flush all stores and sinks
	for _, task := range t.tasks {
		if err := task.Flush(ctx); err != nil {
			return err
		}
		t.log.Info("Flushed task", "task", task)
	}

	// Phase 2: Commit offsets to broker
	if err := t.commit(ctx); err != nil {
		return err
	}

	// Phase 3: CRITICAL - Write checkpoints after successful commit
	// This ensures checkpoint reflects committed offsets
	// On restart, we'll resume from checkpoint (not from beginning)
	for _, task := range t.tasks {
		if err := task.Checkpoint(ctx); err != nil {
			t.log.Error("Failed to checkpoint task", "task", task, "error", err)
			// Don't fail the commit if checkpoint fails - offsets are already committed
			// This matches Kafka Streams behavior: checkpoint errors are logged but not thrown
		} else {
			t.log.Debug("Checkpointed task", "task", task)
		}
	}

	t.log.Info("Committed all tasks")
	return nil
}

func (t *TaskManager) commit(ctx context.Context) error {
	data := t.GetCommittableOffsets()
	errCh := make(chan error, 1)

	// Use the provided context (with its deadline/cancellation) instead of Background
	t.client.CommitOffsetsSync(ctx, data, func(c *kgo.Client, ocr1 *kmsg.OffsetCommitRequest, ocr2 *kmsg.OffsetCommitResponse, e error) {
		if e != nil {
			errCh <- e
			return
		}

		for _, t := range ocr2.Topics {
			for _, p := range t.Partitions {
				err := kerr.ErrorForCode(p.ErrorCode)
				if err != nil {
					errCh <- err
					return
				}
			}
		}

		errCh <- nil

	})

	err := <-errCh
	if err != nil {
		return err
	}

	for _, task := range t.tasks {
		task.ClearOffsets()
	}

	return nil
}

func (t *TaskManager) Close(ctx context.Context) error {
	var err error

	for _, task := range t.tasks {
		err = errors.Join(err, task.Close(ctx))
	}

	return err
}

func (t *TaskManager) TaskFor(topic string, partition int32) (*Task, error) {
	for _, task := range t.tasks {
		if slices.Contains(task.topics, topic) && task.partition == partition {
			return task, nil
		}
	}

	return nil, ErrTaskNotFound
}

var ErrTaskNotFound = errors.New("task not found")
