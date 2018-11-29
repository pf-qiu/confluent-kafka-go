package kafka

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>

*/
import "C"

type TopicConsumer struct {
	consumers  []*LegacyConsumer
	partitions map[int32]int
	topic      string
}

func (tc *TopicConsumer) Close() error {
	for _, c := range tc.consumers {
		c.Close()
	}
	return nil
}

func (tc *TopicConsumer) ConsumeStart(partition int32, offset Offset) error {
	p, ok := tc.partitions[partition]
	if !ok {
		return newErrorFromString(0, "partition not found")
	}
	return tc.consumers[p].ConsumeStart(partition, offset)
}

func (tc *TopicConsumer) ConsumeStop(partition int32) error {
	p, ok := tc.partitions[partition]
	if !ok {
		return newErrorFromString(0, "partition not found")
	}
	return tc.consumers[p].ConsumeStop(partition)
}

func (tc *TopicConsumer) Poll(timeoutMs int, partition int32) (event Event) {
	p, ok := tc.partitions[partition]
	if !ok {
		return newErrorFromString(0, "partition not found")
	}
	return tc.consumers[p].Poll(timeoutMs, partition)
}

func (tc *TopicConsumer) PollBatch(timeoutMs int, partition int32) []Event {
	p, ok := tc.partitions[partition]
	if !ok {
		return []Event{
			newErrorFromString(0, "partition not found"),
		}
	}
	return tc.consumers[p].PollBatch(timeoutMs, partition)
}

func NewTopicConsumer(conf *ConfigMap, batchSize int, topic string) (*TopicConsumer, error) {
	meta, err := NewMetaConsumer(conf, topic)
	if err != nil {
		return nil, err
	}

	c := &TopicConsumer{
		partitions: make(map[int32]int),
		topic:      topic,
	}

	for len(meta.PartitionMap) > 0 {
		cid := len(c.consumers)
		lc, err := NewLegacyBatchConsumer(conf, batchSize, topic)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.consumers = append(c.consumers, lc)

		for broker, partitions := range meta.PartitionMap {
			l := len(partitions) - 1
			p := partitions[l]
			c.partitions[p] = cid
			if l == 0 {
				delete(meta.PartitionMap, broker)
			} else {
				meta.PartitionMap[broker] = partitions[:l]
			}
		}
	}
	return c, nil
}
