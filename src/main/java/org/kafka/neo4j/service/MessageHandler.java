package org.kafka.neo4j.service;

/**
 * Created by jfd on 4/16/17.
 */
public interface MessageHandler {

    String transformMessage(Long offset, String inputMessage) throws Exception;

    void postToNeo4j(String inputMessage, String topic, String id);
}
