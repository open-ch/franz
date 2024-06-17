package franz

import (
	"github.com/IBM/sarama"
)

type Producer struct {
	client sarama.SyncProducer
	codec  *avroCodec
}

func (p *Producer) SendMessage(topic, msg, key string) error {
	_, _, err := p.client.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(msg),
	})

	return err
}

// SendMessageEncoded encodes and sends the JSON format msg with Avro serialization
func (p *Producer) SendMessageEncoded(topic, msg, key string, schemaID uint32) error {
	encoded, err := p.codec.Encode([]byte(msg), schemaID)
	if err != nil {
		return err
	}
	_, _, err = p.client.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(encoded),
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

	return &Producer{client: producer, codec: f.codec}, nil
}
