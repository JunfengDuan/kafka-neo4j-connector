package org.kafka.neo4j.service;

import org.kafka.neo4j.job.KafkaConsumerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jfd on 4/16/17.
 */
public class MessageHandlerImpl implements MessageHandler{

    private static final Logger logger = LoggerFactory.getLogger(MessageHandlerImpl.class);
    private static ConsumeMessageService consumeMessageService = new ConsumeMessageService();

    @Override
    public String transformMessage(Long offset, String inputMessage) throws Exception {
        return inputMessage;
    }

    @Override
    public void addMessageToBatch(String inputMessage, String topic, String id) throws Exception {
        logger.info("Add messages-{} to batch",inputMessage);
        consumeMessageService.addToBulkRequest(inputMessage, topic);
    }

    @Override
    public void upDateMessageToBatch(String inputMessage, String topic, String id) throws Exception {
        logger.info("Update messages-{} to batch",inputMessage);
    }

    @Override
    public void postToNeo4j() {
        logger.info("Post messages-{} to neo4j","");
        consumeMessageService.postToNeo4j();
    }
}
