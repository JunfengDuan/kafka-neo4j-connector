package org.kafka.neo4j.service;

import org.kafka.neo4j.client.Neo4jClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jfd on 4/16/17.
 */
public class ConsumeMessageService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeMessageService.class);
    private String inputMessage;
    private String topic;
    private Neo4jClient neo4jClient = new Neo4jClient();

    public void addToBulkRequest(String inputMessage, String topic) {
        this.inputMessage = inputMessage;
        this.topic = topic;
    }

    public void postToNeo4j() {
        logger.info("Starting post messages {} to neo4j, topic is: {}",inputMessage, topic);
        neo4jClient.makeGraph(topic,inputMessage);
    }
}
