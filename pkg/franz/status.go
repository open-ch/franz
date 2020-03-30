package franz

import "sort"

type Status struct {
	Topic     string
	Partition int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
}

func (f *Franz) Status() ([]Status, error) {
	var states []Status
	topics, err := f.client.Topics()
	if err != nil {
		return nil, err
	}

	for _, topic := range topics {
		err := f.client.RefreshMetadata(topic)
		if err != nil {
			return nil, err
		}

		partitions, err := f.client.Partitions(topic)
		if err != nil {
			return nil, err
		}

		sort.Slice(partitions, func(i int, j int) bool {
			return partitions[i] < partitions[j]
		})

		for _, partition := range partitions {
			isr, err := f.client.InSyncReplicas(topic, partition)
			if err != nil {
				return nil, err
			}

			replicas, err := f.client.Replicas(topic, partition)
			if err != nil {
				return nil, err
			}

			leader, err := f.client.Leader(topic, partition)
			if err != nil {
				return nil, err
			}

			states = append(states, Status{
				Topic:     topic,
				Partition: partition,
				Leader:    leader.ID(),
				Replicas:  replicas,
				ISR:       isr,
			})
		}
	}

	return states, nil
}
