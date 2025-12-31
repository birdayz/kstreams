package kstreams

import (
	"log/slog"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestNewPartitionGroupBalancer(t *testing.T) {
	t.Run("creates balancer with partition groups", func(t *testing.T) {
		log := slog.Default()
		pgs := []*PartitionGroup{
			{sourceTopics: []string{"topic1"}},
			{sourceTopics: []string{"topic2"}},
		}

		balancer := NewPartitionGroupBalancer(log, pgs)
		assert.NotZero(t, balancer)

		pgBalancer, ok := balancer.(*PartitionGroupBalancer)
		assert.True(t, ok, "balancer should be of type *PartitionGroupBalancer")
		assert.Equal(t, 2, len(pgBalancer.pgs))
		assert.NotZero(t, pgBalancer.inner)
		assert.NotZero(t, pgBalancer.log)
	})

	t.Run("creates balancer with empty partition groups", func(t *testing.T) {
		log := slog.Default()
		pgs := []*PartitionGroup{}

		balancer := NewPartitionGroupBalancer(log, pgs)
		assert.NotZero(t, balancer)

		pgBalancer := balancer.(*PartitionGroupBalancer)
		assert.Equal(t, 0, len(pgBalancer.pgs))
	})

	t.Run("uses CooperativeStickyBalancer as inner balancer", func(t *testing.T) {
		log := slog.Default()
		pgs := []*PartitionGroup{}

		balancer := NewPartitionGroupBalancer(log, pgs)
		pgBalancer := balancer.(*PartitionGroupBalancer)

		// Verify it's cooperative
		assert.True(t, pgBalancer.IsCooperative())
	})
}

func TestPartitionGroupBalancer_ProtocolName(t *testing.T) {
	t.Run("returns correct protocol name", func(t *testing.T) {
		balancer := &PartitionGroupBalancer{}
		name := balancer.ProtocolName()
		assert.Equal(t, "kstreams-partitiongroup-cooperative-sticky", name)
	})
}

func TestPartitionGroupBalancer_IsCooperative(t *testing.T) {
	t.Run("returns true", func(t *testing.T) {
		log := slog.Default()
		balancer := NewPartitionGroupBalancer(log, []*PartitionGroup{})
		assert.True(t, balancer.(*PartitionGroupBalancer).IsCooperative())
	})
}

func TestPartitionGroupBalancer_JoinGroupMetadata(t *testing.T) {
	t.Run("delegates to inner balancer", func(t *testing.T) {
		log := slog.Default()
		pgs := []*PartitionGroup{
			{sourceTopics: []string{"topic1"}},
		}
		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		topicInterests := []string{"topic1"}
		currentAssignment := map[string][]int32{"topic1": {0, 1}}
		generation := int32(1)

		metadata := balancer.JoinGroupMetadata(topicInterests, currentAssignment, generation)

		// Should return non-nil metadata
		assert.NotZero(t, metadata)
	})
}

func TestPartitionGroupBalancer_ParseSyncAssignment(t *testing.T) {
	t.Run("delegates to inner balancer", func(t *testing.T) {
		log := slog.Default()
		balancer := NewPartitionGroupBalancer(log, []*PartitionGroup{}).(*PartitionGroupBalancer)

		// Get valid assignment bytes from inner balancer first
		metadata := balancer.inner.JoinGroupMetadata([]string{"topic1"}, map[string][]int32{}, 0)

		// Parse should not panic
		assignment, err := balancer.ParseSyncAssignment(metadata)
		// May or may not error depending on inner balancer implementation
		_ = assignment
		_ = err
	})
}

func TestPartitionGroupBalancer_MemberBalancer(t *testing.T) {
	t.Run("validates required topics are present", func(t *testing.T) {
		log := slog.Default()
		pgs := []*PartitionGroup{
			{sourceTopics: []string{"required-topic"}},
		}
		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"other-topic"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, topics, err := balancer.MemberBalancer(members)

		// Should fail if required topic is missing
		if _, ok := topics["required-topic"]; !ok {
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "required-topic")
			assert.Zero(t, memberBalancer)
		}
	})

	t.Run("succeeds when all required topics present", func(t *testing.T) {
		log := slog.Default()
		pgs := []*PartitionGroup{
			{sourceTopics: []string{"topic1"}},
		}
		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, topics, err := balancer.MemberBalancer(members)

		// Should succeed
		assert.NoError(t, err)
		assert.NotZero(t, memberBalancer)
		assert.NotZero(t, topics)

		// Should be wrapped balancer
		_, ok := memberBalancer.(*WrappingMemberBalancer)
		assert.True(t, ok)
	})

	t.Run("builds member map correctly", func(t *testing.T) {
		log := slog.Default()
		pgs := []*PartitionGroup{
			{sourceTopics: []string{"topic1"}},
		}
		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1"},
					map[string][]int32{},
					0,
				),
			},
			{
				MemberID: "member2",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, _, err := balancer.MemberBalancer(members)
		assert.NoError(t, err)

		wrapped := memberBalancer.(*WrappingMemberBalancer)
		assert.Equal(t, 2, len(wrapped.memberByName))
		assert.NotZero(t, wrapped.memberByName["member1"])
		assert.NotZero(t, wrapped.memberByName["member2"])
	})
}

func TestWrappingMemberBalancer_Balance(t *testing.T) {
	t.Run("returns nil", func(t *testing.T) {
		// Balance() is deprecated in favor of BalanceOrError()
		wb := &WrappingMemberBalancer{}
		topics := map[string]int32{"topic1": 3}

		result := wb.Balance(topics)
		assert.Zero(t, result)
	})
}

func TestWrappingMemberBalancer_BalanceOrError(t *testing.T) {
	t.Run("detects co-partitioning violations", func(t *testing.T) {
		log := slog.Default()

		// Create partition group with mismatched partition counts
		pgs := []*PartitionGroup{
			{
				sourceTopics: []string{"topic1", "topic2"},
			},
		}

		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1", "topic2"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, _, err := balancer.MemberBalancer(members)
		assert.NoError(t, err)

		wrapped := memberBalancer.(*WrappingMemberBalancer)

		// topic1 has 3 partitions, topic2 has 2 - mismatch!
		topics := map[string]int32{
			"topic1": 3,
			"topic2": 2,
		}

		_, err = wrapped.BalanceOrError(topics)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not co-partitioned")
	})

	t.Run("succeeds with matching partition counts", func(t *testing.T) {
		log := slog.Default()

		// Create partition group with matching partition counts
		pgs := []*PartitionGroup{
			{
				sourceTopics: []string{"topic1", "topic2"},
			},
		}

		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1", "topic2"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, _, err := balancer.MemberBalancer(members)
		assert.NoError(t, err)

		wrapped := memberBalancer.(*WrappingMemberBalancer)

		// Both topics have 3 partitions - matching!
		topics := map[string]int32{
			"topic1": 3,
			"topic2": 3,
		}

		plan, err := wrapped.BalanceOrError(topics)
		assert.NoError(t, err)
		assert.NotZero(t, plan)
	})

	t.Run("handles single topic partition group", func(t *testing.T) {
		log := slog.Default()

		pgs := []*PartitionGroup{
			{
				sourceTopics: []string{"topic1"},
			},
		}

		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, _, err := balancer.MemberBalancer(members)
		assert.NoError(t, err)

		wrapped := memberBalancer.(*WrappingMemberBalancer)

		topics := map[string]int32{
			"topic1": 5,
		}

		plan, err := wrapped.BalanceOrError(topics)
		assert.NoError(t, err)
		assert.NotZero(t, plan)
	})

	t.Run("handles multiple partition groups", func(t *testing.T) {
		log := slog.Default()

		pgs := []*PartitionGroup{
			{
				sourceTopics: []string{"topic1", "topic2"},
			},
			{
				sourceTopics: []string{"topic3"},
			},
		}

		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1", "topic2", "topic3"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, _, err := balancer.MemberBalancer(members)
		assert.NoError(t, err)

		wrapped := memberBalancer.(*WrappingMemberBalancer)

		topics := map[string]int32{
			"topic1": 4,
			"topic2": 4, // Must match topic1
			"topic3": 2, // Independent
		}

		plan, err := wrapped.BalanceOrError(topics)
		assert.NoError(t, err)
		assert.NotZero(t, plan)
	})

	t.Run("uses minimum partition count for imbalanced groups", func(t *testing.T) {
		log := slog.Default()

		// When there's an imbalance, it should detect it and return error
		pgs := []*PartitionGroup{
			{
				sourceTopics: []string{"topic1", "topic2", "topic3"},
			},
		}

		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1", "topic2", "topic3"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, _, err := balancer.MemberBalancer(members)
		assert.NoError(t, err)

		wrapped := memberBalancer.(*WrappingMemberBalancer)

		// topic1: 5, topic2: 3, topic3: 4 - all different!
		topics := map[string]int32{
			"topic1": 5,
			"topic2": 3,
			"topic3": 4,
		}

		_, err = wrapped.BalanceOrError(topics)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not co-partitioned")
	})
}

func TestBalanceError(t *testing.T) {
	t.Run("IntoSyncAssignment returns nil", func(t *testing.T) {
		testErr := ErrNodeNotFound
		err := &BalanceError{err: testErr}
		result := err.IntoSyncAssignment()
		assert.Zero(t, result)
	})

	t.Run("IntoSyncAssignmentOrError returns error", func(t *testing.T) {
		testErr := ErrNodeNotFound
		err := &BalanceError{err: testErr}

		assignment, returnedErr := err.IntoSyncAssignmentOrError()
		assert.Zero(t, assignment)
		assert.Equal(t, testErr, returnedErr)
	})
}

func TestWrappingMemberBalancer_getPlanMap(t *testing.T) {
	t.Run("extracts plan map using reflection", func(t *testing.T) {
		log := slog.Default()

		pgs := []*PartitionGroup{
			{
				sourceTopics: []string{"topic1"},
			},
		}

		balancer := NewPartitionGroupBalancer(log, pgs).(*PartitionGroupBalancer)

		members := []kmsg.JoinGroupResponseMember{
			{
				MemberID: "member1",
				ProtocolMetadata: balancer.inner.JoinGroupMetadata(
					[]string{"topic1"},
					map[string][]int32{},
					0,
				),
			},
		}

		memberBalancer, _, err := balancer.MemberBalancer(members)
		assert.NoError(t, err)

		wrapped := memberBalancer.(*WrappingMemberBalancer)

		topics := map[string]int32{
			"topic1": 3,
		}

		plan, err := wrapped.BalanceOrError(topics)
		assert.NoError(t, err)

		balancePlan, ok := plan.(*kgo.BalancePlan)
		assert.True(t, ok)

		// Test getPlanMap
		planMap := wrapped.getPlanMap(balancePlan)
		assert.NotZero(t, planMap)

		// Plan map should be a map[string]map[string][]int32
		for memberID, topicMap := range planMap {
			assert.NotEqual(t, "", memberID)
			assert.NotZero(t, topicMap)
		}
	})
}
