package org.kafka.neo4j.job;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kafka.neo4j.exception.FailedEventsLogger;
import org.kafka.neo4j.service.MessageHandler;
import org.kafka.neo4j.service.MessageHandlerImpl;
import org.kafka.neo4j.util.OffsetLoggingCallbackImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jfd on 4/16/17.
 */
public class KafkaConsumerJob implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerJob.class);
    private MessageHandler messageHandler;
    private int consumerId;
    private long pollIntervalMs;
    private KafkaConsumer consumer;
    private List<String> kafkaTopics;
    private OffsetLoggingCallbackImpl offsetLoggingCallback;

    public KafkaConsumerJob(int consumerId, List<String> kafkaTopics, KafkaConsumer consumer, long pollIntervalMs){
        this.consumerId = consumerId;
        this.kafkaTopics = kafkaTopics;
        this.consumer = consumer;
        this.pollIntervalMs = pollIntervalMs;
        messageHandler = new MessageHandlerImpl();
        offsetLoggingCallback = new OffsetLoggingCallbackImpl();
        logger.info("Created ConsumerWorker with properties: consumerId={}, kafkaTopic={}", consumerId, kafkaTopics);
    }

    @Override
    public void run() {
        try {
            logger.info("Starting KafkaConsumerJob, consumerId={}", consumerId);
            consumer.subscribe(kafkaTopics, offsetLoggingCallback);
            
            while (true){

                int numProcessedMessages = 0;
                int numSkippedIndexingMessages = 0;
                int numMessagesInBatch = 0;
                long offsetOfNextBatch = 0;

                logger.debug("consumerId={}; about to call consumer.poll() ...", consumerId);
                ConsumerRecords<String, String> records = consumer.poll(pollIntervalMs);
                Map<Integer, Long> partitionOffsetMap = new HashMap<>();
                records.forEach(record ->processedMessage(record, numMessagesInBatch, partitionOffsetMap,
                        numProcessedMessages, numSkippedIndexingMessages));

                logger.info("Total {} of messages in this batch; {} of successfully transformed and added to Index; {} of skipped from indexing; offsetOfNextBatch: {}",
                        numMessagesInBatch, numProcessedMessages, numSkippedIndexingMessages, offsetOfNextBatch);

                // push to neo4j whole batch
                boolean moveToNextBatch = false;
                if (!records.isEmpty()) {
                    moveToNextBatch = aboutPostToNeo4j();
                }

                if (moveToNextBatch) {
                    logger.info("Invoking commit for partition/offset : {}", partitionOffsetMap);
                    consumer.commitAsync(offsetLoggingCallback);
                }
            }
            
        } catch (Exception e) {
            logger.error("KafkaConsumerJob [consumerId={}] got Exception - exiting ... Exception: {}", consumerId, e.getMessage());
        } finally {
            logger.warn("KafkaConsumerJob [consumerId={}] is shutting down ...", consumerId);
            consumer.close();
        }

    }

    /**
     * Process kafka message before post to neo4j
     * @param record one kafka message
     * @param numMessagesInBatch
     * @param partitionOffsetMap map of messages's offsets
     * @param numProcessedMessages
     * @param numSkippedIndexingMessages
     */
    private void processedMessage(ConsumerRecord record, int numMessagesInBatch, Map partitionOffsetMap,
                                  int numProcessedMessages, int numSkippedIndexingMessages){
        numMessagesInBatch++;
        Map<String, Object> data = new HashMap<>();
        data.put("partition", record.partition());
        data.put("offset", record.offset());
        data.put("value", record.value());

        logger.debug("consumerId={}; recieved record: {}", consumerId, data);

        try {
            String processedMessage = messageHandler.transformMessage(record.offset(), (String) record.value());
            addOrUpdateMessageToBatch(processedMessage, record.topic(), (String) record.key());
            partitionOffsetMap.put(record.partition(), record.offset());
            numProcessedMessages++;
            
        } catch (Exception e) {
            numSkippedIndexingMessages++;
            logger.error("ERROR processing message {} - skipping it: {}", record.offset(), record.value());
            FailedEventsLogger.logFailedToTransformEvent(record.offset(), e.getMessage(), (String)record.value());
        }
    }

    /**
     * Pre-process message, add message to batch task
     * @param processedMessage
     * @param topic
     * @param id
     * @throws Exception
     */
    private void addOrUpdateMessageToBatch(String processedMessage, String topic, String id) throws Exception{

        com.alibaba.fastjson.JSONObject json = com.alibaba.fastjson.JSON.parseObject(processedMessage);
        if(true){
            logger.info("Add message-{} to batch", processedMessage);
            messageHandler.addMessageToBatch(processedMessage, topic, id);
        }else{
            logger.info("Update message-{} to batch", processedMessage);
            messageHandler.upDateMessageToBatch(processedMessage, topic, id);
        }
    }

    /**
     * Post kafka message to neo4j
     * @return
     * @throws Exception
     */
    private boolean aboutPostToNeo4j() throws Exception{
        boolean moveToTheNextBatch = true;
        try {
            messageHandler.postToNeo4j();
        } catch (Exception e) {
            moveToTheNextBatch = false;
            logger.error("Error posting messages to Neo4j - will re-try processing the batch; error: {}", e.getMessage());
        }

        return moveToTheNextBatch;
    }

    public void shutdown() {
        logger.warn("ConsumerWorker [consumerId={}] shutdown() is called  - will call consumer.wakeup()", consumerId);
        consumer.wakeup();
    }

    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMap() {
        return offsetLoggingCallback.getPartitionOffsetMap();
    }

    public int getConsumerId() {
        return consumerId;
    }
}
