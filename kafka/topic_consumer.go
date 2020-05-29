package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"strconv"
	"strings"
	"sync"
)

// most of the code is from here https://github.com/Shopify/sarama/blob/master/tools/kafka-console-consumer/kafka-console-consumer.go
func ConsumeTopic(config *sarama.Config, servers []string, topic string, partitions string, getPartitionOffset func(partition int32) int64, onMessage func(msg *sarama.ConsumerMessage), onDone func()) (func(), error) {
	consumer, err := sarama.NewConsumer(servers, config)
	if err != nil {
		return nil, err
	}

	partitionList, err := getPartitions(consumer, topic, partitions)
	if err != nil {
		return nil, err
	}

	var (
		closing = make(chan struct{})
		wg      sync.WaitGroup
	)

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, getPartitionOffset(partition))
		if err != nil {
			return nil, err
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				onMessage(message)
			}
		}(pc)
	}

	go func() {
		wg.Wait()
		onDone()
	}()

	close := func() {
		close(closing)
		if err := consumer.Close(); err != nil {
			fmt.Println("Failed to close consumer: ", err)
		}
	}
	return close, nil
}

func getPartitions(c sarama.Consumer, topic string, partitions string) ([]int32, error) {
	if partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}
