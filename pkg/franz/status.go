package franz

import (
	"sort"
)

type Status struct {
	Topic     string
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
	Internal  bool
}

func (f *Franz) Status() ([]Status, error) {
	var states []Status
	topics, err := f.client.Topics()
	if err != nil {
		return nil, err
	}

	topicMetadata, err := f.admin.DescribeTopics(topics)
	if err != nil {
		return nil, err
	}

	for _, topic := range topicMetadata {
		err := f.client.RefreshMetadata(topic.Name)
		if err != nil {
			return nil, err
		}

		partitions, err := f.client.Partitions(topic.Name)
		if err != nil {
			return nil, err
		}

		sort.Slice(partitions, func(i int, j int) bool {
			return partitions[i] < partitions[j]
		})

		for _, partition := range topic.Partitions {
			states = append(states, Status{
				Topic:     topic.Name,
				Partition: partition.ID,
				Leader:    partition.Leader,
				Replicas:  partition.Replicas,
				ISR:       partition.Isr,
				Internal:  topic.IsInternal,
			})
		}
	}

	return states, nil
}
