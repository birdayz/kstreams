package internal

import (
	"fmt"
	"reflect"
	"unsafe"

	"github.com/davecgh/go-spew/spew"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"golang.org/x/exp/slices"
)

// PartitionGroupBalancer is a balancer that uses kgo's Cooperative-sticky balancer under the hood,
// but enforces co-partitioning as defined by the given PartitionGroups.
type PartitionGroupBalancer struct {
	inner kgo.GroupBalancer
	pgs   []*PartitionGroup
}

func NewPartitionGroupBalancer(pgs []*PartitionGroup) kgo.GroupBalancer {
	return &PartitionGroupBalancer{inner: kgo.CooperativeStickyBalancer(), pgs: pgs}
}

func (w *PartitionGroupBalancer) ProtocolName() string {
	return "streamz-partitiongroup-cooperative-sticky"
}

func (w *PartitionGroupBalancer) JoinGroupMetadata(
	topicInterests []string,
	currentAssignment map[string][]int32,
	generation int32,
) []byte {
	return w.inner.JoinGroupMetadata(topicInterests, currentAssignment, generation)
}

func (w *PartitionGroupBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	return w.inner.ParseSyncAssignment(assignment)
}

func (w *PartitionGroupBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (b kgo.GroupMemberBalancer, topics map[string]struct{}, err error) {
	mx := map[string]*kmsg.JoinGroupResponseMember{}
	for i, member := range members {
		mx[member.MemberID] = &members[i]

	}
	innerBalancer, topics, err := w.inner.MemberBalancer(members)

	// Check topics
	for _, pg := range w.pgs {
		for _, requiredTopic := range pg.sourceTopics {
			if _, ok := topics[requiredTopic]; !ok {
				return nil, nil, fmt.Errorf("partition group requires topic %s, but it's missing", requiredTopic)
			}
		}
	}

	wrappedBalancer := &WrappingMemberBalancer{inner: innerBalancer, pgs: w.pgs, memberByName: mx}
	return wrappedBalancer, topics, err
}

func (w *PartitionGroupBalancer) IsCooperative() bool {
	return w.inner.IsCooperative()
}

type WrappingMemberBalancer struct {
	inner kgo.GroupMemberBalancer
	pgs   []*PartitionGroup

	memberByName map[string]*kmsg.JoinGroupResponseMember
}

func (wb *WrappingMemberBalancer) Balance(topics map[string]int32) kgo.IntoSyncAssignment {
	firstTopics := make([]string, 0, len(wb.pgs))
	additionals := map[string][]string{} // firstTopics => rest

	for _, pg := range wb.pgs {
		slices.Sort(pg.sourceTopics)
		firstTopics = append(firstTopics, pg.sourceTopics[0])
		if len(pg.sourceTopics) > 1 {
			additionals[pg.sourceTopics[0]] = pg.sourceTopics[1:]
		}
	}

	strippedMap := make(map[string]int32)

	for _, topic := range firstTopics {
		strippedMap[topic] = topics[topic]
	}

	spew.Dump(strippedMap)

	plan := wb.inner.Balance(strippedMap)

	balancePlan, ok := plan.(*kgo.BalancePlan)
	if !ok {
		panic("invalid balance plan type, this should not happen and indicates an incompatibility with franz-go")
	}

	planMap := wb.getPlanMap(balancePlan)

	for member, memberMap := range planMap {
		for topic, partitions := range memberMap {
			if moreTopics, ok := additionals[topic]; ok {
				for _, otherTopic := range moreTopics {
					balancePlan.AddPartitions(wb.memberByName[member], otherTopic, partitions)
				}
			}

		}

	}

	return plan
}

func (wb *WrappingMemberBalancer) getPlanMap(i *kgo.BalancePlan) map[string]map[string][]int32 {
	planField := reflect.ValueOf(i).Elem().FieldByName("plan")
	planField = reflect.NewAt(planField.Type(), unsafe.Pointer(planField.UnsafeAddr())).Elem()
	planMap, ok := planField.Interface().(map[string]map[string][]int32)
	if !ok {
		panic("could not cast to balance plan map, this should not happen and indicates an incompatibility with franz-go")
	}

	return planMap

}
