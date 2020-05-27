package kafka

import "github.com/Shopify/sarama"

func CreateProducer(servers []string) (MessageSender, error) {
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	return CreateProducerWithConfig(servers, config)
}

func CreateProducerWithConfig(servers []string, config *sarama.Config) (MessageSender, error) {
	producer, err := sarama.NewSyncProducer(servers, config)
	return simpleKafkaSender{prd: producer}, err
}

type MessageSender interface {
	Send(topic string, bytes []byte) error
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
