package franz

import (
	"strings"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type TopicDiff struct {
	ToCreate []Topic          `json:"to_create" yaml:"to_create"`
	ToDelete []Topic          `json:"to_delete" yaml:"to_delete"`
	ToAlter  []AlteredConfigs `json:"to_alter" yaml:"to_alter"`
}

// Topic represents a Kafka topic with its configuration
type Topic struct {
	Name              string               `json:"name" yaml:"name"`
	NumPartitions     int                  `json:"num_partitions" yaml:"partitions"`
	ReplicationFactor int                  `json:"replication_factor" yaml:"replication"`
	Configs           []sarama.ConfigEntry `json:"configs" yaml:"configs"`
}

type AlteredConfigs struct {
	TopicName string
	Configs   map[string]*string
}

func (f *Franz) GetTopicsExisting(includeInternal bool) ([]Topic, error) {
	clusterAdmin, err := f.getClusterAdmin()
	if err != nil {
		return nil, errors.Wrap(err, "creation of sarama cluster admin failed")
	}

	f.log.Info("retrieving existing topics")
	topics, err := clusterAdmin.GetTopicsExisting(f.client, includeInternal)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve existing topics")
	}

	return topics, nil
}

func (f *Franz) GetTopicsDiff(topics []Topic) (TopicDiff, error) {
	clusterAdmin, err := f.getClusterAdmin()
	if err != nil {
		return TopicDiff{}, errors.Wrap(err, "cluster admin creation failed")
	}

	f.log.Infof("getting existing topics...")
	topicsExisting, err := clusterAdmin.GetTopicsExisting(f.client, false)
	if err != nil {
		return TopicDiff{}, errors.Wrap(err, "failed to get existing topics")
	}

	if err := validateTopics(topics); err != nil {
		return TopicDiff{}, err
	}

	toCreate, toDelete, toAlter := diffTopics(topics, topicsExisting)

	return TopicDiff{
		ToCreate: toCreate,
		ToDelete: toDelete,
		ToAlter:  toAlter,
	}, nil
}

func (f *Franz) SetTopics(diff TopicDiff) error {
	clusterAdmin, err := f.getClusterAdmin()
	if err != nil {
		return errors.Wrap(err, "cluster admin creation failed")
	}

	err = clusterAdmin.SetKafkaTopics(diff)
	if err != nil {
		return errors.Wrap(err, "failed to set topics from file")
	}

	return nil
}

// GetTopicsExisting retrieves the topic names and configuration parameters
// The configuration parameters are filtered - only the non-default are returned
func (c *ClusterAdmin) GetTopicsExisting(client sarama.Client, includeInternal bool) ([]Topic, error) {
	topicNames, err := client.Topics()
	if err != nil {
		return nil, err
	}

	var topics []Topic
	for _, topicName := range topicNames {
		if !includeInternal && isInternal(topicName) {
			continue
		}

		configs, err := c.getTopicConfig(topicName)
		if err != nil {
			return nil, err
		}
		partitionsCount, replicationFactor, err := getPartitionsAndReplicationFactor(client, topicName)
		if err != nil {
			return nil, err
		}
		nonDefaultConfigs := filterNonDefault(configs)

		topics = append(topics, Topic{
			Name:              topicName,
			Configs:           nonDefaultConfigs,
			NumPartitions:     partitionsCount,
			ReplicationFactor: replicationFactor,
		})
	}

	return topics, nil
}

func getPartitionsAndReplicationFactor(client sarama.Client, topicName string) (numPartitions int, numReplicas int, err error) {
	partitions, err := client.Partitions(topicName)
	if err != nil {
		return -1, -1, err
	}
	// assume that all partitions have the same replication factor
	replicas, err := client.Replicas(topicName, partitions[0])
	if err != nil {
		return -1, -1, err
	}
	return len(partitions), len(replicas), nil
}

func filterNonDefault(configs []sarama.ConfigEntry) (filtered []sarama.ConfigEntry) {
	for _, config := range configs {
		if !config.Default {
			filtered = append(filtered, config)
		}
	}
	return filtered
}

func (c *ClusterAdmin) getTopicConfig(topicName string) ([]sarama.ConfigEntry, error) {
	searchConfig := sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topicName,
	}

	configEntry, err := c.client.DescribeConfig(searchConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get topic configuration values")
	}

	return configEntry, nil
}

// SetKafkaTopics creates/alters/deletes the topics in Kafka to reach the configuration given in the file.
func (c *ClusterAdmin) SetKafkaTopics(diff TopicDiff) error {
	c.log.Printf("Deleting %d topics", len(diff.ToDelete))
	for _, topic := range diff.ToDelete {
		err := c.client.DeleteTopic(topic.Name)
		if err != nil {
			return errors.Wrap(err, "failed to delete topic")
		}
	}

	c.log.Printf("Setting %d topics", len(diff.ToCreate))
	for _, topic := range diff.ToCreate {
		configs := configToMap(topic)
		topicDetail := sarama.TopicDetail{
			NumPartitions:     int32(topic.NumPartitions),
			ReplicationFactor: int16(topic.ReplicationFactor),
			ConfigEntries:     configs,
		}
		err := c.client.CreateTopic(topic.Name, &topicDetail, false)
		if err != nil {
			return errors.Wrap(err, "failed to create topic")
		}
	}

	c.log.Printf("Altering %d topics...", len(diff.ToAlter))
	for _, alterConf := range diff.ToAlter {
		c.log.Printf("Altering the configuration of topic %s...", alterConf.TopicName)
		err := c.client.AlterConfig(sarama.TopicResource, alterConf.TopicName, alterConf.Configs, false)
		if err != nil {
			return errors.Wrap(err, "failed to alter config of topic")
		}
	}

	return nil
}

// diffTopics goes through the new and existing topics and figures out which topics have to be created and which need to be altered
func diffTopics(topicsNew, topicsExisting []Topic) (toCreate, toDelete []Topic, toAlter []AlteredConfigs) {
	// initially all given topics are candidates to be created
	toCreate = append(toCreate, topicsNew...)
	// initially all existing topics are candidates to be removed
	toDelete = append(toDelete, topicsExisting...)

	for _, topic := range topicsNew {
		// check if there is a topic with this name
		contains, index := containsTopic(topicsExisting, topic)
		if contains {
			// check the config differences of the topics
			alteredTopic, altered := diffTopicsConfig(topic, topicsExisting[index])
			if altered {
				toAlter = append(toAlter, alteredTopic)
			}
			toCreate = delTopicFromArray(topic, toCreate)
			toDelete = delTopicFromArray(topic, toDelete)
		}
	}

	if toAlter == nil {
		toAlter = []AlteredConfigs{}
	}

	return toCreate, toDelete, toAlter
}

// diffTopicsConfig compares the configuration of two topics
// and returns the configs that need to be changed in the second topic to be the same as the first
func diffTopicsConfig(newConfig, oldConfig Topic) (AlteredConfigs, bool) {
	alteredConf := AlteredConfigs{}
	alteredConf.TopicName = newConfig.Name
	alteredConf.Configs = make(map[string]*string)
	altered := false
	for i, param := range newConfig.Configs {
		oldValue, exists := getParamFromName(oldConfig, param.Name)
		// don't change the config value if it is the same
		if (exists && param.Value != *oldValue) || !exists {
			alteredConf.Configs[param.Name] = &newConfig.Configs[i].Value
			altered = true
		}
	}
	return alteredConf, altered
}

func getParamFromName(topic Topic, paramName string) (*string, bool) {
	for _, p := range topic.Configs {
		if p.Name == paramName {
			return &p.Value, true
		}
	}
	return nil, false
}

func configToMap(topic Topic) map[string]*string {
	configs := make(map[string]*string)
	for _, conf := range topic.Configs {
		tmp := conf.Value
		configs[conf.Name] = &tmp
	}
	return configs
}

func delTopicFromArray(topic Topic, topics []Topic) []Topic {
	index := topicIndex(topic, topics)
	if index == -1 {
		return topics
	}
	topics = append(topics[:index], topics[index+1:]...)
	return topics
}

func topicIndex(topic Topic, topics []Topic) int {
	_, i := containsTopic(topics, topic)
	return i
}

func containsTopic(topics []Topic, topic Topic) (exists bool, index int) {
	for i, v := range topics {
		if v.Name == topic.Name {
			return true, i
		}
	}
	return false, -1
}

func validateTopics(topics []Topic) error {
	for _, topic := range topics {
		if isInternal(topic.Name) {
			return errors.Errorf(`topic name "%s" must not contain "_" as this is reserved for internal names`, topic.Name)
		}
	}

	return nil
}

func isInternal(topicName string) bool {
	return strings.HasPrefix(topicName, "_")
}
