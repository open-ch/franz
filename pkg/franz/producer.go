package franz

import (
	"github.com/Shopify/sarama"
)

type Producer struct {
	client sarama.SyncProducer
}

func (p *Producer) SendMessage(topic, msg, key string) error {
	_, _, err := p.client.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(msg),
	})

	return err
}

func (p *Producer) SendMessageBytes(topic string, msg []byte, key string) error {
	_, _, err := p.client.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(msg),
	})

	return err
}

func (p *Producer) Close() error {
	return p.client.Close()
}

func (f *Franz) NewProducer() (*Producer, error) {
	producer, err := sarama.NewSyncProducerFromClient(f.client)
	if err != nil {
		return nil, err
	}

	return &Producer{client: producer}, nil
}
