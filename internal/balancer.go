package internal

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type streamzBalancer struct {
	topics []string
}

func (b *streamzBalancer) ProtocolName() string {
	return "streamz"
}

func (b *streamzBalancer) JoinGroupMetadata(
	topicInterests []string,
	currentAssignment map[string][]int32,
	generation int32,
) []byte {
	fmt.Println("join")
	// TODO
	return []byte{}
}

// ParseSyncAssignment returns assigned topics and partitions from an
// encoded SyncGroupResponse's MemberAssignment.
func (b *streamzBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
	spew.Dump(assignment)
	return kgo.ParseConsumerSyncAssignment(assignment)
}

func (b *streamzBalancer) MemberTopics() map[string]struct{} {
	res := map[string]struct{}{}

	for _, topic := range b.topics {
		res[topic] = struct{}{}
	}
	return res
}

func (b *streamzBalancer) MemberBalancer(members []kmsg.JoinGroupResponseMember) (
	bal kgo.GroupMemberBalancer,
	topics map[string]struct{},
	err error) {
	fmt.Println("try")
	bala, err := kgo.NewConsumerBalancer(b, members)
	fmt.Println(err)
	return bala, b.MemberTopics(), err
}

func (b *streamzBalancer) Balance(*kgo.ConsumerBalancer, map[string]int32) kgo.IntoSyncAssignment {
	fmt.Println("ZZ")
	p := &kgo.BalancePlan{}
	return p
}

// IsCooperative returns if this is a cooperative balance strategy.
func (b *streamzBalancer) IsCooperative() bool {
	return false
}
