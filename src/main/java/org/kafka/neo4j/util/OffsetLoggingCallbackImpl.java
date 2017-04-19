package org.kafka.neo4j.util;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dhyan on 12/9/16.
 */
public class OffsetLoggingCallbackImpl implements OffsetCommitCallback, ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(OffsetLoggingCallbackImpl.class);
    private Map<TopicPartition, OffsetAndMetadata> partitionOffsetMap = new ConcurrentHashMap<>();

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception == null) {
            offsets.forEach((topicPartition, offsetAndMetadata) -> {
                partitionOffsetMap.computeIfPresent(topicPartition, (k, v) -> offsetAndMetadata);
                logger.info("Offset position during the commit for consumerId : {}, partition : {}, offset : {}", Thread.currentThread().getName(), topicPartition.partition(), offsetAndMetadata.offset());
            });
        } else {
            offsets.forEach((topicPartition, offsetAndMetadata) -> logger.error("Offset commit error, and partition offset info : {}, partition : {}, offset : {}", exception.getMessage(), topicPartition.partition(), offsetAndMetadata.offset()));
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("Partitions revoked : {}", partitions);
        for (TopicPartition currentPartition : partitions) {
            partitionOffsetMap.remove(currentPartition);
        }

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("Partitions assigned : {}", partitions);
        for (TopicPartition currentPartition : partitions) {
            partitionOffsetMap.put(currentPartition,new OffsetAndMetadata(0L, "Initial default offset"));
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return partitionOffsetMap;
    }
}
