package kafka

import "github.com/Shopify/sarama"

type MessageSender interface {
	Send(topic string, bytes []byte) error
}

func CreateProducerWithConfig(config *sarama.Config, servers []string) (MessageSender, error) {
	producer, err := sarama.NewSyncProducer(servers, config)
	return simpleKafkaSender{prd: producer}, err
}

type simpleKafkaSender struct {
	prd sarama.SyncProducer
}

func (s simpleKafkaSender) Send(topic string, bytes []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(bytes),
	}
	_, _, err := s.prd.SendMessage(msg)
	return err
}
