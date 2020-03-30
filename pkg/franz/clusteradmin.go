package franz

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type ClusterAdmin struct {
	client sarama.ClusterAdmin
	log    logrus.FieldLogger
}

// NewClient returns an acl Client
func newClusterAdmin(client sarama.Client, log logrus.FieldLogger) (*ClusterAdmin, error) {
	clusterAdmin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		return nil, err
	}

	return &ClusterAdmin{
		client: clusterAdmin,
		log:    log,
	}, nil
}

func (c *ClusterAdmin) close() error {
	return c.client.Close()
}
