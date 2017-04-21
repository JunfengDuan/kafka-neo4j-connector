package org.kafka.neo4j.job;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.kafka.neo4j.client.KafkaConsumerClient;
import org.kafka.neo4j.exception.FailedEventsLogger;
import org.kafka.neo4j.service.MessageHandler;
import org.kafka.neo4j.service.MessageHandlerImpl;
import org.kafka.neo4j.util.OffsetLoggingCallbackImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jfd on 4/16/17.
 */
public class KafkaConsumerJob implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerJob.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private MessageHandler messageHandler;
    private String consumerId;
    private long pollIntervalMs;
    private KafkaConsumer consumer;
    private List<String> kafkaTopics;
    private OffsetLoggingCallbackImpl offsetLoggingCallback;
    private int numMessagesInBatch = 0;
    private int numProcessedMessages = 0;
    private int numSkippedIndexingMessages = 0;

    public KafkaConsumerJob(Properties kafkaProperties, String consumerId, List<String> kafkaTopics, long pollIntervalMs){
        this.consumerId = consumerId;
        this.kafkaTopics = kafkaTopics;
        this.consumer = new KafkaConsumer(kafkaProperties);
        this.pollIntervalMs = pollIntervalMs;
        messageHandler = new MessageHandlerImpl();
        offsetLoggingCallback = new OffsetLoggingCallbackImpl();
        logger.info("Created KafkaConsumerJob with properties: consumerId={}, kafkaTopic={}", consumerId, kafkaTopics);
    }

    @Override
    public void run() {
        try {
            logger.info("Starting KafkaConsumerJob, consumerId={}", consumerId);
            consumer.subscribe(kafkaTopics, offsetLoggingCallback);
            
            while (true){
                numMessagesInBatch = 0;
                numProcessedMessages = 0;
                numSkippedIndexingMessages = 0;
                long offsetOfNextBatch = 0;

                logger.debug("consumerId={}; about to call consumer.poll() ...", consumerId);
                ConsumerRecords<String, String> records = consumer.poll(pollIntervalMs);
                Map<Integer, Long> partitionOffsetMap = new HashMap<>();

                // push to neo4j whole batch
                boolean moveToNextBatch = false;
                if (!records.isEmpty()) {
                    moveToNextBatch = aboutPostToNeo4j(records, partitionOffsetMap);
                }


                logger.info("Total {} of messages in this batch; {} of successfully transformed and added to Index; {} of skipped from indexing; offsetOfNextBatch: {}",
                        numMessagesInBatch, numProcessedMessages, numSkippedIndexingMessages, offsetOfNextBatch);

                if (moveToNextBatch) {
                    logger.info("Invoking commit for partition/offset : {}", partitionOffsetMap);
                    consumer.commitAsync(offsetLoggingCallback);
                }

                List<String> newTopics = topicIsChanged();
                if(newTopics.size()>0){
                    logger.info("Kafka topics are changed, new topics are :{}",kafkaTopics);
                    consumer.subscribe(kafkaTopics, offsetLoggingCallback);
                    consumer.commitSync(getPartitionAndOffset(newTopics.get(0),0));
                }
            }
            
        } catch (WakeupException e) {
            logger.error("KafkaConsumerJob [consumerId={}] got WakeupException - exiting ... Exception: {}", consumerId, e.getMessage());
        } catch (Exception e){
            logger.error("KafkaConsumerJob [consumerId={}] got java.lang.Exception - {}", consumerId, e.getMessage());
        }
        finally {
            logger.warn("KafkaConsumerJob [consumerId={}] is shutting down ...", consumerId);
            consumer.close();
        }

    }

    /**
     * Process kafka message before post to neo4j
     * @param record one kafka message
     * @param partitionOffsetMap map of messages's offsets
     */
    private void processedMessage(ConsumerRecord record, Map partitionOffsetMap){
        numMessagesInBatch++;
        Map<String, Object> data = new HashMap<>();
        data.put("partition", record.partition());
        data.put("offset", record.offset());
        data.put("value", record.value());

        logger.debug("consumerId={}; received record: {}", consumerId, data);

        try {
            String processedMessage = messageHandler.transformMessage(record.offset(), (String) record.value());
            messageHandler.postToNeo4j(processedMessage, record.topic(), (String) record.key());
            partitionOffsetMap.put(record.partition(), record.offset());
            numProcessedMessages++;
            
        } catch (Exception e) {
            numSkippedIndexingMessages++;
            logger.error("ERROR processing message {} - skipping it: {}", record.offset(), record.value());
            FailedEventsLogger.logFailedToTransformEvent(record.offset(), e.getMessage(), (String)record.value());
        }
    }

    /**
     * Post kafka message to neo4j
     * @return
     * @throws Exception
     */
    private boolean aboutPostToNeo4j(ConsumerRecords<String, String> records, Map<Integer, Long> partitionOffsetMap){
        boolean moveToTheNextBatch = true;
        try {
            records.forEach(record ->processedMessage(record, partitionOffsetMap));
        } catch (Exception e) {
            moveToTheNextBatch = false;
            logger.error("Error posting messages to Neo4j - will re-try processing the batch; error: {}", e.getMessage());
        }

        return moveToTheNextBatch;
    }

    private List<String> topicIsChanged(){
        KafkaConsumerClient kafkaConsumerClient = new KafkaConsumerClient();
        List<String> topics = kafkaConsumerClient.getKafkaTopics(consumer);
        List<String> remain = new ArrayList<>();
        if(topics.size() > kafkaTopics.size()){
            remain.addAll(topics);
            remain.removeAll(kafkaTopics);
            kafkaTopics.addAll(topics);
        }
        return remain;
    }

    private Map getPartitionAndOffset(String topic, long offset){
        Map map = new HashMap();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        PartitionInfo partitionInfo = partitionInfos.get(0);
//        KafkaConsumerClient kafkaConsumerClient = new KafkaConsumerClient();
//        kafkaConsumerClient.createConsumerInstance(partitionInfo.toString(), Arrays.asList(topic));
        map.put(new TopicPartition(partitionInfo.topic(),partitionInfo.partition()),new OffsetAndMetadata(offset));
        return map;
    }

    public void shutdown() {
        logger.warn("ConsumerWorker [consumerId={}] shutdown() is called  - will call consumer.wakeup()", consumerId);
        closed.set(true);
        consumer.wakeup();
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return offsetLoggingCallback.getPartitionOffsetMap();
    }

    public String getConsumerId() {
        return consumerId;
    }
}
