package kafka

import (
	"fmt"
	"sync"
	"unsafe"
)

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>

typedef struct message_contiainer
{
	rd_kafka_message_t** msgs;
	size_t count;
} message_contiainer;

message_contiainer* new_container(size_t count) {
	message_contiainer* container = malloc(sizeof(message_contiainer));
	container->msgs = malloc(sizeof(rd_kafka_message_t*) * count);
	container->count = count;
	return container;
}

void destroy_container(message_contiainer* container) {
	free(container->msgs);
	container->msgs = 0;
	free(container);
}

ssize_t consume_messages(rd_kafka_topic_t *rkt, int32_t partition,
				int timeout_ms, message_contiainer* container)
{
	return rd_kafka_consume_batch(rkt, partition, timeout_ms,
		container->msgs, container->count);
}

void destroy_messages(message_contiainer *container, ssize_t count)
{
	for (ssize_t i = 0; i < count; i++)
	{
		rd_kafka_message_destroy(container->msgs[i]);
	}
}
*/
import "C"

type LegacyConsumer struct {
	rk         *C.rd_kafka_t
	rkt        *C.rd_kafka_topic_t
	topic      string
	batchSize  int
	containers map[int32]*C.message_contiainer
	l          sync.Mutex
}

// Strings returns a human readable name for a Consumer instance
func (c *LegacyConsumer) String() string {
	return fmt.Sprintf("LegacyConsumer(Topic: %s, BatchSize: %v, Partitions: %v)",
		c.topic, c.batchSize, len(c.containers))
}

func (c *LegacyConsumer) Close() error {
	if c.rkt != nil {
		C.rd_kafka_topic_destroy(c.rkt)
		C.rd_kafka_consumer_close(c.rk)
		for partition := range c.containers {
			c.ConsumeStop(partition)
		}
	}
	C.rd_kafka_destroy(c.rk)

	return nil
}

func (c *LegacyConsumer) ConsumeStop(partition int32) error {
	c.l.Lock()
	defer c.l.Unlock()
	if C.rd_kafka_consume_stop(c.rkt, C.int32_t(partition)) != 0 {
		return newErrorFromString(ErrorCode(C.rd_kafka_last_error()), "rd_kafka_consume_stop failed")
	}
	cont := c.containers[partition]
	C.destroy_container(cont)
	delete(c.containers, partition)
	return nil
}

func (c *LegacyConsumer) ConsumeStart(partition int32, offset Offset) error {
	c.l.Lock()
	defer c.l.Unlock()
	if C.rd_kafka_consume_start(c.rkt, C.int32_t(partition), C.int64_t(offset)) != 0 {
		return newErrorFromString(ErrorCode(C.rd_kafka_last_error()), "rd_kafka_consume_start failed")
	}
	c.containers[partition] = C.new_container(C.ulong(c.batchSize))
	return nil
}

func (c *LegacyConsumer) Poll(timeoutMs int, partition int32) (event Event) {
	if c.rkt == nil {
		return nil
	}
	C.rd_kafka_poll(c.rk, C.int(0))
	cmsg := C.rd_kafka_consume(c.rkt, C.int32_t(partition), C.int(timeoutMs))
	if cmsg != nil {
		defer C.rd_kafka_message_destroy(cmsg)
		return c.buildMessage(cmsg)
	}
	return nil
}

func (c *LegacyConsumer) PollBatch(timeoutMs int, partition int32) []Event {
	if c.rkt == nil {
		return nil
	}

	if int(partition) > len(c.containers) || partition < 0 {
		return []Event{Error{
			code: -1,
			str:  "invalid partition",
		}}
	}

	container := c.containers[partition]
	C.rd_kafka_poll(c.rk, C.int(0))

	count := C.consume_messages(c.rkt, C.int32_t(partition), C.int(timeoutMs), container)
	if count > 0 {
		defer C.destroy_messages(container, count)
		events := make([]Event, count)
		messages := c.getMessage(container.msgs, int(count))
		for i := 0; i < int(count); i++ {
			events[i] = c.buildMessage(messages[i])
		}
		return events
	}
	return nil
}

func (c *LegacyConsumer) getMessage(msgs **C.rd_kafka_message_t, count int) []*C.rd_kafka_message_t {
	slice := (*[1 << 30]*C.rd_kafka_message_t)(unsafe.Pointer(msgs))[:count:count]
	return slice
}

func (c *LegacyConsumer) buildMessage(cmsg *C.rd_kafka_message_t) Event {
	if cmsg.err == 0 {
		msg := &Message{}
		msg.TopicPartition.Topic = &c.topic
		msg.TopicPartition.Partition = int32(cmsg.partition)
		msg.TopicPartition.Offset = Offset(cmsg.offset)
		if cmsg.payload != nil {
			msg.Value = C.GoBytes(unsafe.Pointer(cmsg.payload), C.int(cmsg.len))
		}
		if cmsg.key != nil {
			msg.Key = C.GoBytes(unsafe.Pointer(cmsg.key), C.int(cmsg.key_len))
		}
		return msg
	} else if cmsg.err == C.RD_KAFKA_RESP_ERR__PARTITION_EOF {
		return PartitionEOF{
			Topic:     &c.topic,
			Partition: int32(cmsg.partition),
			Offset:    Offset(cmsg.offset),
		}
	} else {
		return newError(cmsg.err)
	}
}

func NewLegacyBatchConsumer(conf *ConfigMap, batchSize int, topic string) (*LegacyConsumer, error) {
	err := versionCheck()
	if err != nil {
		return nil, err
	}

	c := &LegacyConsumer{
		topic:      topic,
		containers: make(map[int32]*C.message_contiainer),
	}

	cConf, err := conf.convert()
	if err != nil {
		return nil, err
	}
	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	c.rk = C.rd_kafka_new(C.RD_KAFKA_CONSUMER, cConf, cErrstr, 256)
	if c.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	c.batchSize = batchSize

	ctopic := C.CString(topic)
	c.rkt = C.rd_kafka_topic_new(c.rk, ctopic, nil)
	C.free(unsafe.Pointer(ctopic))

	return c, nil
}
