package org.kafka.neo4j.service;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import java.util.List;
import java.util.Map;

/**
 * Created by jfd on 4/17/17.
 */
public interface Neo4jHandler {

    /**
     * Add node
     * @param session
     * @param cypher
     * @param params
     * @return
     */
    StatementResult createNode(Session session, String cypher, Map<String, Object> params);

    /**
     * Add node relationship
     * @param session
     * @param cypher
     * @return
     */
    StatementResult createNodeRelation(Session session, String cypher);

    /**
     * Add unique constraint
     * @param session
     * @param cypher
     */
    void createIndexOrUniqueConstraint(Session session, String cypher);

    /**
     * Delete one node
     * @param session
     * @param cypher
     */
    void deleteNode(Session session, String cypher);

    /**
     * Delete a node with all its relationships
     * @param session
     * @param cypher
     */
    void deleteAllNodesWithRelationships(Session session, String cypher);

    void updateNode(Session session, Map cypher);

}
