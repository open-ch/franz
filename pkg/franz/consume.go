package franz

import (
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/IBM/sarama"
)

type Result struct {
	err error
	msg Message
}

type Receiver struct {
	cancel             context.CancelFunc
	ctx                context.Context
	messageC           chan Result
	availableConsumers int
}

type MonitorRequest struct {
	Topic      string
	Partitions []int32
	Count      int64
	Follow     bool
	Decode     bool
}

type HistoryRequest struct {
	Topic      string
	From, To   time.Time // To takes precedence over Count
	Count      int64
	Partitions []int32
	Decode     bool
}

// Stop instructs the receiver to finish receiving messages.
// Does not block until all goroutines have finished. Instead,
// Next() can be called to drain the remaining messages and
// wait until all goroutines have finished.
func (r *Receiver) Stop() {
	r.cancel()
}

// Next retrieves the next message. If there are no more messages,
// io.EOF is returned. Not thread-safe.
func (r *Receiver) Next() (Message, error) {
	for result := range r.messageC {
		if result.err == io.EOF {
			r.availableConsumers--
			if r.availableConsumers == 0 {
				return Message{}, io.EOF
			}
			continue
		} else if result.err != nil {
			return Message{}, result.err
		}

		return result.msg, nil
	}

	return Message{}, io.EOF
}

func (f *Franz) Monitor(req MonitorRequest) (*Receiver, error) {
	if req.Count <= 0 {
		return nil, errors.New("desired message count needs to be larger than 0")
	}

	if len(req.Partitions) == 0 {
		p, err := f.client.Partitions(req.Topic)
		if err != nil {
			return nil, err
		}

		req.Partitions = p
	}

	ctx, cancel := context.WithCancel(context.Background())

	rec := Receiver{
		cancel:             cancel,
		ctx:                ctx,
		messageC:           make(chan Result),
		availableConsumers: len(req.Partitions),
	}

	for _, partition := range req.Partitions {
		partition := partition // capture variable locally for go-routines

		go func() {
			if err := f.consume(&rec, req.Topic, partition, req.Count, req.Follow, req.Decode); err != nil {
				if errors.Is(err, ErrNoMessages) {
					f.log.Warnf("no messages available on partition %d", partition)
				} else if err != nil {
					f.log.Error(err)
				}
			}
		}()
	}

	return &rec, nil
}

func (f *Franz) HistoryEntries(req HistoryRequest) ([]Message, error) {
	if len(req.Partitions) == 0 {
		p, err := f.client.Partitions(req.Topic)
		if err != nil {
			return nil, err
		}

		req.Partitions = p
	}

	consumer, err := sarama.NewConsumerFromClient(f.client)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	messages := make([]Message, 0)
	for _, partition := range req.Partitions {
		startOffset, err := f.client.GetOffset(req.Topic, partition, req.From.UnixNano()/int64(time.Millisecond))
		if err != nil {
			return nil, err
		}

		if startOffset == sarama.OffsetNewest {
			f.log.Warnf("no messages available on partition %d", partition)
			continue
		}

		// endOffset refers to the message that will be produced next,
		// i.e. we stop at the message received one before.
		endOffset := sarama.OffsetNewest
		if !req.To.Equal(time.Time{}) {
			// Use absolute time

			offset, err := f.client.GetOffset(req.Topic, partition, req.To.UnixNano()/int64(time.Millisecond))
			if err != nil {
				return nil, err
			}

			endOffset = offset
		} else if req.Count > 0 {
			// Use offset count

			endOffset = startOffset + req.Count

			latestOffset, err := f.client.GetOffset(req.Topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}

			if endOffset > latestOffset {
				endOffset = latestOffset
			}
		}

		if endOffset == sarama.OffsetNewest {
			offset, err := f.client.GetOffset(req.Topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, err
			}

			endOffset = offset
		}

		partitionConsumer, err := consumer.ConsumePartition(req.Topic, partition, startOffset)
		if err != nil {
			return nil, err
		}

		for message := range partitionConsumer.Messages() {
			messageTransformed := Message{
				Topic:     message.Topic,
				Timestamp: message.Timestamp,
				Partition: message.Partition,
				Key:       string(message.Key),
				Value:     string(message.Value),
				Offset:    message.Offset,
			}

			if req.Decode {
				decoded, err := f.codec.Decode(message.Value)
				if err != nil {
					return nil, err
				}

				messageTransformed.Value = string(decoded)
			}

			messages = append(messages, messageTransformed)

			if (message.Offset + 1) == endOffset {
				if err := partitionConsumer.Close(); err != nil {
					f.log.Error(err)
				}

				// drain remaining messages
				for range partitionConsumer.Messages() {
				}
			}
		}
	}

	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp.Before(messages[j].Timestamp)
	})

	return messages, nil
}

func (f *Franz) consume(receiver *Receiver, topic string, partition int32, count int64, follow, decode bool) error {
	defer func() {
		receiver.messageC <- Result{err: io.EOF}
	}()

	offsetNewest, err := f.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return err
	}

	offsetOldest, err := f.client.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return err
	}

	if offsetOldest == offsetNewest {
		return ErrNoMessages
	}

	offsetStart := offsetNewest - count
	if offsetStart < offsetOldest {
		offsetStart = sarama.OffsetOldest
	}

	offsetEnd := offsetNewest - 1
	if follow {
		offsetEnd = sarama.OffsetNewest
	}

	f.log.Infof("starting consumer for partition %d at offsetNewest %d", partition, offsetStart)

	consumer, err := sarama.NewConsumerFromClient(f.client)
	if err != nil {
		return err
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, partition, offsetStart)
	if err != nil {
		return err
	}
	defer pc.Close()

	for {
		select {
		case <-receiver.ctx.Done():
			return nil

		case message := <-pc.Messages():
			msg := Message{
				Topic:     message.Topic,
				Timestamp: message.Timestamp,
				Partition: message.Partition,
				Key:       string(message.Key),
				Value:     string(message.Value),
				Offset:    message.Offset,
			}

			if decode {
				decoded, err := f.codec.Decode(message.Value)
				if err != nil {
					return err
				}

				msg.Value = string(decoded)
			}

			select {
			case <-receiver.ctx.Done():
				return nil
			case receiver.messageC <- Result{msg: msg}:
			}

			if msg.Offset == offsetEnd {
				return nil
			}
		}
	}
}
