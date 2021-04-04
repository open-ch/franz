package franz

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Config struct {
	KafkaVersion   string
	Brokers        []string
	SchemaRegistry string
	*TLSConfig
}

type TLSConfig struct {
	CertFile, KeyFile, CaFile string
}

type Message struct {
	Topic      string
	Timestamp  time.Time
	Partition  int32
	Key, Value string
	Offset     int64
}

type Franz struct {
	brokers      []string
	client       sarama.Client
	admin        sarama.ClusterAdmin
	log          logrus.FieldLogger
	registry     Registry
	codec        *avroCodec
	clusterAdmin *ClusterAdmin // use Franz.admin directly instead of clusterAdmin
}

func New(c Config, verbose bool) (*Franz, error) {
	sc, err := parseConfig(c)
	if err != nil {
		return nil, err
	}

	log, err := createLogger(verbose)
	if err != nil {
		return nil, err
	}

	if len(c.Brokers) == 0 {
		return nil, errors.New("no broker set")
	}

	client, err := sarama.NewClient(c.Brokers, sc)
	if err != nil {
		return nil, err
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}

	registry := Registry(nilRegistry{})
	if c.SchemaRegistry != "" {
		registry, err = newRegistry(c, log)
		if err != nil {
			return nil, err
		}
	}

	return &Franz{
		brokers:  c.Brokers,
		log:      log,
		client:   client,
		admin:    admin,
		registry: registry,
		codec:    newAvroCodec(registry),
	}, nil
}

func (f *Franz) Close() error {
	if f.clusterAdmin != nil {
		f.clusterAdmin.close()
		f.clusterAdmin = nil
	}

	f.admin.Close()

	err := f.client.Close()
	f.client = nil

	return err
}

func (f *Franz) Registry() Registry {
	return f.registry
}

func (f *Franz) Codec() avroCodec {
	return *f.codec
}

func (f *Franz) getClusterAdmin() (*ClusterAdmin, error) {
	if f.clusterAdmin == nil {
		clusterAdmin, err := newClusterAdmin(f.client, f.log)
		if err != nil {
			return nil, err
		}

		f.clusterAdmin = clusterAdmin
	}

	return f.clusterAdmin, nil
}
