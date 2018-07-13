package kafka

import (
	"fmt"
	"unsafe"
)

/*
#include <librdkafka/rdkafka.h>
#include <stdlib.h>
*/
import "C"

type LegacyConsumer struct {
	rk        *C.rd_kafka_t
	rkt       *C.rd_kafka_topic_t
	partition int32
	topic     string
}

// Strings returns a human readable name for a Consumer instance
func (c *LegacyConsumer) String() string {
	return "LegacyConsumer"
}

func (c *LegacyConsumer) Close() error {
	if c.rkt != nil {
		C.rd_kafka_consumer_close(c.rk)
	}
	C.rd_kafka_destroy(c.rk)
	return nil
}

func (c *LegacyConsumer) Unassign() error {
	return nil
}

func (c *LegacyConsumer) Assign(topics []TopicPartition) error {
	if len(topics) != 1 {
		return fmt.Errorf("Only support one topic")
	}
	topic := topics[0]
	err := c.consumeStart(topic.Topic, topic.Partition, topic.Offset)
	if err != nil {
		return err
	}
	c.topic = *topic.Topic

	return nil
}

func (c *LegacyConsumer) consumeStart(topic *string, partition int32, offset Offset) error {
	if c.rkt != nil {
		return Error{ErrConflict, "consumer already started"}
	}
	c.partition = partition
	c.topic = *topic

	ctopic := C.CString(c.topic)
	c.rkt = C.rd_kafka_topic_new(c.rk, ctopic, nil)
	C.free(unsafe.Pointer(ctopic))
	if C.rd_kafka_consume_start(c.rkt, C.int32_t(partition), C.int64_t(offset)) != 0 {
		return newErrorFromString(ErrorCode(C.rd_kafka_last_error()), "rd_kafka_consume_start failed")
	}

	return nil
}

func (c *LegacyConsumer) consumeStop() {
	if c.rkt != nil {
		C.rd_kafka_consume_stop(c.rkt, C.int32_t(c.partition))
		C.rd_kafka_topic_destroy(c.rkt)
		c.rkt = nil
	}
}

func (c *LegacyConsumer) Poll(timeoutMs int) (event Event) {
	if c.rkt == nil {
		return nil
	}
	C.rd_kafka_poll(c.rk, C.int(0))
	cmsg := C.rd_kafka_consume(c.rkt, C.int32_t(c.partition), C.int(timeoutMs))
	if cmsg != nil {
		defer C.rd_kafka_message_destroy(cmsg)
		return c.buildMessage(cmsg)
	}
	return nil
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

		//crktpar := C.rd_kafka_event_topic_partition(rkev)
		return PartitionEOF{
			Topic:     &c.topic,
			Partition: int32(cmsg.partition),
			Offset:    Offset(cmsg.offset),
		}
	} else {
		return newError(cmsg.err)
	}
}

func NewLegacyConsumer(conf *ConfigMap) (*LegacyConsumer, error) {
	err := versionCheck()
	if err != nil {
		return nil, err
	}

	c := &LegacyConsumer{}

	cConf, err := conf.convert()
	if err != nil {
		return nil, err
	}
	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	C.rd_kafka_conf_set_events(cConf, C.RD_KAFKA_EVENT_REBALANCE|C.RD_KAFKA_EVENT_OFFSET_COMMIT|C.RD_KAFKA_EVENT_STATS)

	c.rk = C.rd_kafka_new(C.RD_KAFKA_CONSUMER, cConf, cErrstr, 256)
	if c.rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}

	return c, nil
}
