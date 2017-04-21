package org.kafka.neo4j.service;

import org.kafka.neo4j.client.Neo4jClient;
import org.kafka.neo4j.job.KafkaConsumerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jfd on 4/16/17.
 */
public class MessageHandlerImpl implements MessageHandler{

    private static final Logger logger = LoggerFactory.getLogger(MessageHandlerImpl.class);
    private Neo4jClient neo4jClient = new Neo4jClient();

    @Override
    public String transformMessage(Long offset, String inputMessage) throws Exception {
        return inputMessage;
    }

    @Override
    public void postToNeo4j(String inputMessage, String topic, String id) {
        logger.info("Starting post messages {} to neo4j, topic is: {}",inputMessage, topic);
        neo4jClient.makeGraph(topic,inputMessage);
    }
}
