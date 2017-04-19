package org.kafka.neo4j.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Created by jfd on 4/1/17.
 */
@Component
public class KafkaConsumerUtil {

    @Value("${consumerGroupName:kafka-neo4j-consumer}")
    private  String consumerGroupName;
    @Value("${consumerInstanceName:instance1}")
    private  String consumerInstanceName;
    @Value("${kafka.consumer.brokers.list:localhost:9092}")
    private  String kafkaBrokersList;
    @Value("${consumerSessionTimeoutMs:10000}")
    private  int consumerSessionTimeoutMs;
    // Max number of bytes to fetch in one poll request PER partition, default is 1M = 1048576
    @Value("${kafka.consumer.max.partition.fetch.bytes:1048576}")
    private  int maxPartitionFetchBytes;
    // if set to TRUE - enable logging timings of the event processing
    @Value("${isPerfReportingEnabled:false}")
    private  boolean isPerfReportingEnabled;
    @Value("${kafkaReinitSleepTimeMs:5000}")
    private  int kafkaReinitSleepTimeMs;

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerUtil.class);
    private static KafkaConsumer<String, String> kafkaConsumer;
    private static final Properties kafkaProperties = new Properties();

    public KafkaConsumer getConsumer() {
        if (kafkaConsumer == null) {
            kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokersList);
            kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupName);
            kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumerSessionTimeoutMs);
            kafkaProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
            kafkaProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            kafkaConsumer = new KafkaConsumer<>(kafkaProperties);
        }
        return kafkaConsumer;
    }
}


