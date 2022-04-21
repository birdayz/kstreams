package internal

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type streamzBalancer struct {
	log    *zerolog.Logger
	topics []string
	t      *TaskManager
}

func (b *streamzBalancer) ProtocolName() string {
	return "streamz"
}

func (b *streamzBalancer) JoinGroupMetadata(
	interests []string,
	currentAssignment map[string][]int32,
	generation int32,
) []byte {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Version = 0
	meta.Topics = interests // input interests are already sorted
	return meta.AppendTo(nil)

	// meta := kmsg.NewConsumerMemberMetadata()
	// meta.Version = 0
	// meta.Topics = topicInterests
	// spew.Dump(meta.Topics)
	// // if s.cooperative {
	// // 	meta.Version = 1
	// // }
	// // stickyMeta := kmsg.NewStickyMemberMetadata()
	// // stickyMeta.Generation = generation
	// // for topic, partitions := range currentAssignment {
	// // 	if s.cooperative {
	// // 		metaPart := kmsg.NewConsumerMemberMetadataOwnedPartition()
	// // 		metaPart.Topic = topic
	// // 		metaPart.Partitions = partitions
	// // 		meta.OwnedPartitions = append(meta.OwnedPartitions, metaPart)
	// // 	}
	// // 	stickyAssn := kmsg.NewStickyMemberMetadataCurrentAssignment()
	// // 	stickyAssn.Topic = topic
	// // 	stickyAssn.Partitions = partitions
	// // 	stickyMeta.CurrentAssignment = append(stickyMeta.CurrentAssignment, stickyAssn)
	// // }
	// //
	// // // KAFKA-12898: ensure our topics are sorted
	// // metaOwned := meta.OwnedPartitions
	// // stickyCurrent := stickyMeta.CurrentAssignment
	// // sort.Slice(metaOwned, func(i, j int) bool { return metaOwned[i].Topic < metaOwned[j].Topic })
	// // sort.Slice(stickyCurrent, func(i, j int) bool { return stickyCurrent[i].Topic < stickyCurrent[j].Topic })
	// //
	// // meta.UserData = stickyMeta.AppendTo(nil)
	// return meta.AppendTo(nil)
}

// ParseSyncAssignment returns assigned topics and partitions from an
// encoded SyncGroupResponse's MemberAssignment.
func (b *streamzBalancer) ParseSyncAssignment(assignment []byte) (map[string][]int32, error) {
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
	kgo.GroupMemberBalancer,
	map[string]struct{},
	error) {
	balancer, err := kgo.NewConsumerBalancer(b, members)

	allPGTopics := map[string]struct{}{}
	for _, pg := range b.t.pgs {
		for _, topic := range pg.sourceTopics {
			allPGTopics[topic] = struct{}{}
		}
	}

	for t := range allPGTopics {
		if _, ok := balancer.MemberTopics()[t]; !ok {
			b.log.Error().Str("topic_name", t).Msg("Topic missing")
			return nil, nil, fmt.Errorf("topic missing: %s", t)
		}
	}

	// Validate topics

	return balancer, balancer.MemberTopics(), err
}

func (b *streamzBalancer) Balance(x *kgo.ConsumerBalancer, i map[string]int32) kgo.IntoSyncAssignment {

	tps := map[string][]int32{}

	for topic, n := range i {
		partitions := make([]int32, 0, n)
		for d := 0; d < int(n); d++ {
			partitions = append(partitions, int32(d))
		}
		tps[topic] = partitions
	}

	pgp, err := b.t.matchingPGs(tps)
	if err != nil {
		panic(err)
	}

	// Check co-partitioning

	// p := &kgo.BalancePlan{}
	p := x.NewPlan()
	for _, pg := range pgp {

		partitionCount := i[pg.sourceTopics[0]]

		for f := 0; f < int(partitionCount); f++ {

			// Assign all topics to someone
			for _, topic := range pg.sourceTopics {
				p.AddPartition(&x.Members()[f%len(x.Members())], topic, int32(f))
			}
		}

	}

	return p
}

// IsCooperative returns if this is a cooperative balance strategy.
func (b *streamzBalancer) IsCooperative() bool {
	return false
}
