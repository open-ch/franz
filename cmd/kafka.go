package cmd

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type ScrapeConfig struct {
	FetchMinInterval        int
	FetchMaxInterval        int
	MetadataRefreshInterval int
	Topics                  []string
	Groups                  []string
}

func newScrapeConfig(refresh, fetchMin, fetchMax int, topics, groups []string) ScrapeConfig {
	log := getLogger()

	if len(topics) == 0 {
		log.Println("No topics provided")
	}

	if len(groups) == 0 {
		log.Println("No consumer groups provided")
	}

	return ScrapeConfig{
		FetchMinInterval:        fetchMin,
		FetchMaxInterval:        fetchMax,
		MetadataRefreshInterval: refresh,
		Topics:                  topics,
		Groups:                  groups,
	}
}

func startKafkaScraper(wg *sync.WaitGroup, shutdown chan struct{}, client sarama.Client, cfg ScrapeConfig, consumerOffset *prometheus.GaugeVec) {
	go refreshMetadata(wg, shutdown, client, cfg)
	go manageConsumerLag(wg, shutdown, client, cfg, consumerOffset)
}

func refreshMetadata(wg *sync.WaitGroup, shutdown chan struct{}, client sarama.Client, cfg ScrapeConfig) {
	log := getLogger()

	wg.Add(1)
	defer wg.Done()
	wait := time.After(0)
	for {
		select {
		case <-wait:
			err := client.RefreshMetadata()
			if err != nil {
				log.Printf("Failed to update cluster metadata: %s", err)
			}
		case <-shutdown:
			log.Println("Initiating refreshMetadata shutdown of consumer lag broker handler...")
			return
		}
		// FFS this is in nanoseconds...
		delay := time.Duration(cfg.MetadataRefreshInterval) * 1000000000
		wait = time.After(delay)
	}
}

func manageConsumerLag(wg *sync.WaitGroup, shutdown chan struct{}, client sarama.Client, cfg ScrapeConfig, metricOffsetConsumer *prometheus.GaugeVec) {
	log := getLogger()
	groups = getGroups(client, cfg)

	topics = getTopics(client, cfg)
	topicPartitions := map[string][]int32{}
	numberRequests := 0

	for _, topic := range topics {
		parts := fetchPartitions(client, topic)
		topicPartitions[topic] = parts
		numberRequests += len(parts)
	}

	wg.Add(1)
	defer wg.Done()
	wait := time.After(0)
	for {
		select {
		case <-wait:

			requestWG := &sync.WaitGroup{}
			requestWG.Add(2 + numberRequests)
			for _, group := range groups {
				for topic, partitions := range topicPartitions {
					go func(group, topic string, partitions []int32, metricOffsetConsumer *prometheus.GaugeVec) {
						requestWG.Done()
						fetchGroupOffset(client, group, topic, partitions, metricOffsetConsumer)
					}(group, topic, partitions, metricOffsetConsumer)
				}
			}

		case <-shutdown:
			log.Println("Initiating shutdown of consumer lag broker handler...")
			return
		}

		min := int64(cfg.FetchMinInterval)
		max := int64(cfg.FetchMaxInterval)

		duration := time.Duration(rand.Int63n(max-min)) * time.Second

		wait = time.After(duration)
	}
}

func connect(broker *sarama.Broker, cfg *sarama.Config) error {
	ok, _ := broker.Connected()
	if ok {
		return nil
	}

	err := broker.Open(cfg)
	if err != nil {
		return err
	}

	connected, err := broker.Connected()
	if err != nil {
		return err
	} else if !connected {
		return errors.New("unknown failure")
	}

	return nil
}

func fetchGroupOffset(client sarama.Client, grp, tpc string, parts []int32, metric *prometheus.GaugeVec) {
	log := getLogger()
	offsetManager, err := sarama.NewOffsetManagerFromClient(grp, client)
	if err != nil {
		log.Fatalf("Failed to create an offset manager: %v", err)
	}

	defer offsetManager.Close()

	for _, part := range parts {
		pom, err := offsetManager.ManagePartition(tpc, part)
		if err != nil {
			log.Fatalf("failed to manage partition group=%s topic=%s partition=%d err=%v", grp, tpc, part, err)
		}

		defer pom.Close()

		partitionOffset, _ := pom.NextOffset()
		if partitionOffset < 0 {
			log.Printf("Negative value for consumer offset of topic: %s, partition %d", tpc, part)
			break
		}

		newestOffset, err := client.GetOffset(tpc, part, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Failed retrieving newest offset for topic: %s, partition %d", tpc, part)
		}

		consumerLag := newestOffset - partitionOffset

		metric.With(prometheus.Labels{
			"topic":     tpc,
			"partition": strconv.Itoa(int(part)),
			"group":     grp,
		}).Set(float64(consumerLag))
	}
}

func fetchPartitions(client sarama.Client, topic string) []int32 {
	log := getLogger()
	partitions, err := client.Partitions(topic)
	if err != nil {
		log.Fatalf("Failed to retrieve partitions %s", err)
	}

	return partitions
}

func getTopics(client sarama.Client, cfg ScrapeConfig) []string {
	log := getLogger()

	var topics []string
	kafkaTopics, err := client.Topics()
	if err != nil {
		log.Fatalf("Failed to retrieve topics %s", err)
	}

	for _, kafkaTopic := range kafkaTopics {
		if stringInArray(kafkaTopic, cfg.Topics) {
			topics = append(topics, kafkaTopic)
		}
	}
	if len(topics) == 0 {
		log.Fatal("Kafka topics mismatch in config and kafka")
	}

	return topics
}

func getGroups(client sarama.Client, cfg ScrapeConfig) []string {
	log := getLogger()

	var groups []string
	for _, broker := range client.Brokers() {
		err := connect(broker, client.Config())
		if err != nil {
			log.Printf("Failed to connect to broker: %s", err)
			break
		}

		groupsResponse, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			log.Printf("Failed to retrieve consumer groups: %s", err)
		} else if groupsResponse.Err != sarama.ErrNoError {
			log.Printf("Failed to retrieve consumer groups: %s", err)
		} else {
			for group := range groupsResponse.Groups {
				if stringInArray(group, cfg.Groups) {
					groups = append(groups, group)
				}
			}
		}
	}

	if len(groups) == 0 {
		log.Fatal("Consumer groups mismatch in config and kafka")
	}

	return groups
}

func stringInArray(a string, array []string) bool {
	for _, b := range array {
		if a == b {
			return true
		}
	}
	return false
}
