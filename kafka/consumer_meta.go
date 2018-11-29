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

type MetaConsumer struct {
	PartitionMap map[int32][]int32
}

// Strings returns a human readable name for a Consumer instance
func (c *MetaConsumer) String() string {
	return fmt.Sprintf("MetaConsumer")
}

func getBroker(brokers *C.rd_kafka_metadata_broker_t, count int) []C.rd_kafka_metadata_broker_t {
	slice := (*[1 << 30]C.rd_kafka_metadata_broker_t)(unsafe.Pointer(brokers))[:count:count]
	return slice
}

func getPartitions(partitions *C.rd_kafka_metadata_partition_t, count int) []C.rd_kafka_metadata_partition_t {
	slice := (*[1 << 30]C.rd_kafka_metadata_partition_t)(unsafe.Pointer(partitions))[:count:count]
	return slice
}

func NewMetaConsumer(conf *ConfigMap, topic string) (*MetaConsumer, error) {
	err := versionCheck()
	if err != nil {
		return nil, err
	}

	c := &MetaConsumer{
		PartitionMap: make(map[int32][]int32),
	}

	cConf, err := conf.convert()
	if err != nil {
		return nil, err
	}
	cErrstr := (*C.char)(C.malloc(C.size_t(256)))
	defer C.free(unsafe.Pointer(cErrstr))

	rk := C.rd_kafka_new(C.RD_KAFKA_CONSUMER, cConf, cErrstr, 256)
	if rk == nil {
		return nil, newErrorFromCString(C.RD_KAFKA_RESP_ERR__INVALID_ARG, cErrstr)
	}
	defer C.rd_kafka_destroy(rk)
	ctopic := C.CString(topic)
	rkt := C.rd_kafka_topic_new(rk, ctopic, nil)
	C.free(unsafe.Pointer(ctopic))
	defer C.rd_kafka_topic_destroy(rkt)

	var meta *C.rd_kafka_metadata_t
	res := C.rd_kafka_metadata(rk, 0, rkt, &meta, 2000)
	if res != 0 {
		return nil, newErrorFromString(ErrorCode(res), "rd_kafka_metadata failed")
	}
	defer C.rd_kafka_metadata_destroy(meta)

	brokers := getBroker(meta.brokers, int(meta.broker_cnt))
	for i := 0; i < len(brokers); i++ {
		c.PartitionMap[int32(brokers[i].id)] = []int32{}
	}

	partitions := getPartitions(meta.topics.partitions, int(meta.topics.partition_cnt))
	for i := 0; i < len(partitions); i++ {
		id := int32(partitions[i].id)
		leader := int32(partitions[i].leader)
		c.PartitionMap[leader] = append(c.PartitionMap[leader], id)
	}

	return c, nil
}
