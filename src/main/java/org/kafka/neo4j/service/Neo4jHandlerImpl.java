package org.kafka.neo4j.service;

import org.neo4j.driver.v1.*;
import org.slf4j.*;
import java.util.Map;

import static org.kafka.neo4j.util.Cypher.REMOVE;
import static org.kafka.neo4j.util.Cypher.SET;

/**
 * Created by jfd on 4/17/17.
 */
public class Neo4jHandlerImpl implements Neo4jHandler{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(Neo4jHandlerImpl.class);

    @Override
    public StatementResult createNode(Session session, String cypher, Map<String,Object> params) {

        logger.info("Start creating node...");

        StatementResult result;

        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            result = tx.run(cypher, params);
            tx.success();
        }
        return result;
    }

    @Override
    public StatementResult createNodeRelation(Session session, String cypher) {

        logger.info("Start creating relation...");

        StatementResult result;
        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            result = tx.run(cypher);
            tx.success();
        }
        return result;
    }

    @Override
    public void createIndexOrUniqueConstraint(Session session, String indexCypher, String uniqueCypher){

        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            tx.run(indexCypher);
            tx.success();
            logger.info("Index created successfully");
        }
        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            tx.run(uniqueCypher);
            tx.success();
            logger.info("Unique constraint created successfully");
        }
    }

    @Override
    public void updateNode(Session session, Map cypher) {

        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            tx.run((String)cypher.get(REMOVE));
            tx.success();
        }
        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            tx.run((String)cypher.get(SET));
            tx.success();
        }
    }

    @Override
    public StatementResult queryNode(Session session, String cypher) {

        StatementResult result;
        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            result = tx.run(cypher);
            tx.success();
        }
        return result;
    }

    @Override
    public StatementResult queryRelationship(Session session, String cypher) {
        return null;
    }

    @Override
    public void deleteNode(Session session, String cypher) {

        session.run("MATCH (n:Student{ name:{name} }) DELETE n");

    }

    @Override
    public void deleteAllNodesWithRelationships(Session session, String cypher) {

    }
}
