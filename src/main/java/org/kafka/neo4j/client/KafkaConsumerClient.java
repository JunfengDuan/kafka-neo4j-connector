package org.kafka.neo4j.client;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.kafka.neo4j.job.KafkaConsumerJob;
import org.kafka.neo4j.util.KafkaConsumerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import static java.util.stream.Collectors.toList;

/**
 * Created by jfd on 4/16/17.
 */
@Component
public class KafkaConsumerClient {

    @Value("${kafka.consumer.pool.count:5}")
    private  int kafkaConsumerPoolCount;
    // interval in MS to poll Kafka brokers for messages, in case there were no messages during the previous interval
    @Value("${kafkaPollIntervalMs:10000}")
    private  long kafkaPollIntervalMs;
    @Autowired
    private KafkaConsumerUtil kafkaConsumerUtil;
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerClient.class);
    private static final String KAFKA_CONSUMER_THREAD_NAME_FORMAT = "kafka-elasticsearch-consumer-thread-%d";
    private ThreadFactory threadFactory;
    private List<KafkaConsumerJob> consumers;

//    private KafkaConsumer kafkaConsumer;
    private Properties kafkaProperties;
    private static Map<String, List<PartitionInfo>> topicInfo = new HashMap<>();
    private static List<String> kafkaTopics = new ArrayList<>();
    private ExecutorService consumersThreadPool = null;
    private AtomicBoolean running = new AtomicBoolean(false);

    public void init(){
        logger.info("Initializing KafkaConsumerClient...");
        kafkaProperties = kafkaConsumerUtil.getKafkaProperties();
        determineOffsetForAllPartitionsAndSeek();
        initConsumers();
        logger.info("KafkaConsumerClient is Initialized OK");
    }

    private void initConsumers() {
        logger.info("initConsumers() starting...");

        List<List<PartitionInfo>> list = new ArrayList<>(Collections.unmodifiableCollection(topicInfo.values()));
        List<PartitionInfo> partitionInfoList = list.stream().flatMap(List :: stream).filter(p -> !p.topic().startsWith("_")).collect(toList());
        Long partitions = partitionInfoList.stream().map(part -> part.partition()).count();
        int numOfPartitions = Math.max(Integer.parseInt(partitions.toString()),kafkaConsumerPoolCount);

        consumers = new ArrayList<>();
        threadFactory = new ThreadFactoryBuilder().setNameFormat(KAFKA_CONSUMER_THREAD_NAME_FORMAT).build();
        consumersThreadPool = Executors.newFixedThreadPool(numOfPartitions, threadFactory);

        partitionInfoList.forEach(partitionInfo -> {
            createConsumerInstance(partitionInfo.toString(), kafkaTopics);

        });
        logger.info("initConsumers() OK");
    }

    public void createConsumerInstance(String consumerId, List<String> kafkaTopics) {
        KafkaConsumerJob consumerJob = new KafkaConsumerJob(kafkaProperties, consumerId, kafkaTopics, kafkaPollIntervalMs);
        consumers.add(consumerJob);
        consumersThreadPool.submit(consumerJob);
    }

    private void determineOffsetForAllPartitionsAndSeek(){
        KafkaConsumer consumer = new KafkaConsumer(kafkaProperties);
        getKafkaTopics(consumer);
        if (kafkaTopics.size()==0){
            return;
        }
        consumer.subscribe(kafkaTopics);

        //Make init poll to get assigned partitions
        consumer.poll(kafkaPollIntervalMs);
        Set<TopicPartition> assignedTopicPartitions = consumer.assignment();
        assignedTopicPartitions.forEach(topicPartition -> {
            long offsetBeforeSeek = consumer.position(topicPartition);
            logger.info("Offset for partition: {} is moved from : {} to {}", topicPartition.partition(), offsetBeforeSeek, consumer.position(topicPartition));
            logger.info("Offset position during the startup for consumerId : {}, partition : {}, offset : {}",
                    Thread.currentThread().getName(), topicPartition.partition(), consumer.position(topicPartition));
        });
        consumer.commitSync();
        consumer.close();
    }

    public List<String> getKafkaTopics(KafkaConsumer consumer){
        topicInfo = consumer.listTopics();
        kafkaTopics = topicInfo.keySet().stream().filter(key -> !key.startsWith("_")).collect(toList());
        logger.info("kafkaTopics :{}",kafkaTopics);

        return kafkaTopics;
    }

    public void shutdownConsumers() {
        logger.info("shutdownConsumers() started ....");

        if (consumers != null) {
            for (KafkaConsumerJob consumer : consumers) {
                consumer.shutdown();
            }
        }
        if (consumersThreadPool != null) {
            consumersThreadPool.shutdown();
            try {
                consumersThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.warn("Got InterruptedException while shutting down consumers, aborting");
            }
        }
        if (consumers != null) {
            consumers.forEach(consumer -> consumer.getPartitionOffsetMap()
                    .forEach((topicPartition, offset)
                            -> logger.info("Offset position during the shutdown for consumerId : {}, partition : {}, offset : {}", consumer.getConsumerId(), topicPartition.partition(), offset.offset())));
        }
        logger.info("shutdownConsumers() finished");
    }

    @PostConstruct
    public void postConstruct() {
        start();
    }

    @PreDestroy
    public void preDestroy() {
        stop();
    }

    synchronized public void start() {
        if (!running.getAndSet(true)) {
            init();
        } else {
            logger.warn("Already running");
        }
    }

    synchronized public void stop() {
        if (running.getAndSet(false)) {
            shutdownConsumers();
        } else {
            logger.warn("Already stopped");
        }
    }
}
