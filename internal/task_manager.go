package internal

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/multierr"
	"golang.org/x/exp/slices"
)

type TaskManager struct {
	tasks []*Task

	client *kgo.Client
	log    *zerolog.Logger

	topology *TopologyBuilder

	pgs []*PartitionGroup
}

type pgPartitions struct {
	partitionGroup *PartitionGroup
	partitions     []int32
}

func (t *TaskManager) matchingPGs(assignedOrRevoked map[string][]int32) ([]*PartitionGroup, error) {
	var matchingPGs []*PartitionGroup

	// For each pg, check if its required topics are there
	for _, pg := range t.pgs {
		matching := 0
		for _, pgTopic := range pg.sourceTopics {
			if _, ok := assignedOrRevoked[pgTopic]; ok {
				matching++
			}
		}

		if matching == 0 {
			// No TopicPartitions assigned for this PG. that's okay
			continue
		}

		if matching < len(pg.sourceTopics) {
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
		for _, topic := range pg.sourceTopics {
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

	for _, pg := range matchingPGs {
		for _, partition := range pg.partitions {
			task, err := t.topology.CreateTask(pg.partitionGroup.sourceTopics, partition)
			if err != nil {
				return errors.New("failed to create task")
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

	for _, pg := range matchingPGs {
		for _, partition := range pg.partitions {

			// Find task
			found := false
			for i, task := range t.tasks {
				if slices.Equal(task.topics, pg.partitionGroup.sourceTopics) && task.partition == partition {
					found = true
					if err := task.Close(); err != nil {
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

func (t *TaskManager) Commit() error {
	var err error
	for _, task := range t.tasks {
		err = multierr.Append(task.Commit(t.client, t.log), err)
	}
	return err
}

func (t *TaskManager) Close() error {
	var err error
	err = multierr.Append(err, t.Commit())

	for _, task := range t.tasks {
		err = multierr.Append(err, task.Close())
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
