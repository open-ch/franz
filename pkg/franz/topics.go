package franz

import (
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

func (f *Franz) GetTopicsDiff(topics []Topic) (TopicDiff, error) {
	f.log.Infof("getting existing topics...")
	topicsExisting, err := f.GetTopicsExisting(false)
	if err != nil {
		return TopicDiff{}, errors.Wrap(err, "failed to get existing topics")
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
func (f *Franz) GetTopicsExisting(includeInternal bool) ([]Topic, error) {
	topicNames, err := f.client.Topics()
	if err != nil {
		return nil, err
	}

	topicsMetadata, err := f.admin.DescribeTopics(topicNames)
	if err != nil {
		return nil, err
	}

	var topics []Topic
	for _, topic := range topicsMetadata {
		if !includeInternal && topic.IsInternal {
			continue
		}

		configs, err := f.getTopicConfig(topic.Name)
		if err != nil {
			return nil, err
		}

		nonDefaultConfigs := filterNonDefault(configs)

		topicConverted := Topic{
			Name:              topic.Name,
			Configs:           nonDefaultConfigs,
			NumPartitions:     len(topic.Partitions),
			ReplicationFactor: len(topic.Partitions[0].Replicas),
		}

		topics = append(topics, topicConverted)
	}

	return topics, nil
}

func filterNonDefault(configs []sarama.ConfigEntry) (filtered []sarama.ConfigEntry) {
	for _, config := range configs {
		if !config.Default {
			filtered = append(filtered, config)
		}
	}
	return filtered
}

func (f *Franz) getTopicConfig(topicName string) ([]sarama.ConfigEntry, error) {
	searchConfig := sarama.ConfigResource{
		Type: sarama.TopicResource,
		Name: topicName,
	}

	configEntry, err := f.admin.DescribeConfig(searchConfig)
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
	// make sure the slices are at least empty slices and never nil
	toCreate, toDelete, toAlter = []Topic{}, []Topic{}, []AlteredConfigs{}

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

	return toCreate, toDelete, toAlter
}

// diffTopicsConfig compares the configuration of two topics
// and returns the new configs if sth need to be changed in the second topic to be the same as the first
func diffTopicsConfig(newConfig, oldConfig Topic) (AlteredConfigs, bool) {
	alteredConf := AlteredConfigs{}
	alteredConf.TopicName = newConfig.Name
	alteredConf.Configs = make(map[string]*string)
	altered := false
	for i, param := range newConfig.Configs {
		oldValue, exists := getParamFromName(oldConfig, param.Name)
		// copy all the new values to the altered, as Sarama applies the whole list of configs, it will be used only, if sth is changed.
		alteredConf.Configs[param.Name] = &newConfig.Configs[i].Value
		// don't make any change if the config value is the same
		if (exists && param.Value != *oldValue) || !exists {
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
