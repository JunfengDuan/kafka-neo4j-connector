package org.kafka.neo4j.service;

import org.neo4j.driver.v1.*;
import org.slf4j.*;
import java.util.Map;

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
    public StatementResult createNodeRelation(Session session, String cypher, Map<String,Object> params) {

        logger.info("Start creating relation...");

        StatementResult result;
        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            result = tx.run(cypher, params);
            tx.success();
        }
        return result;
    }

    @Override
    public void createIndexOrUniqueConstraint(Session session, String cypher){

        try ( org.neo4j.driver.v1.Transaction tx = session.beginTransaction() )
        {
            tx.run(cypher);
            tx.success();
        }
    }

    @Override
    public void deleteNode(Driver driver, Map<String, Object> params) {
        Session session = driver.session();

        session.run("MATCH (n:Student{ name:{name} }) DELETE n",params);

        session.close();

        driver.close();

    }

    @Override
    public void getAllNode(Driver driver) {
        Session session = driver.session();

        StatementResult result = session.run("START n=node(*) RETURN n.name AS name");

        while (result.hasNext()) {

            Record record = result.next();

            System.out.println(record.get("name").asString());
        }

        session.close();
        driver.close();

    }

    @Override
    public void deleteNodeRelation(Driver driver, Map<String, Object> params) {
        Session session = driver.session();

        session.run("MATCH (a:Student { name:{nameA} })-[r:relName]->(b:Student { name:{nameB} }) DELETE r",params);

        session.close();

        driver.close();

    }

    @Override
    public void searchAllNodeRelation(Driver driver, String relabel) {
        Session session = driver.session();

        StatementResult result = session.run("MERGE (m)-[r:"+relabel+"]->(n) RETURN r.relAttr AS relValue");

        while(result.hasNext()){

            Record record = result.next();

            System.out.println(record.get("relValue").asString());
        }

        session.close();
        driver.close();

    }

    @Override
    public void searchBetweenNodeRelation(Driver driver, Map<String, Object> params) {
        Session session = driver.session();

        StatementResult result = session.run("MERGE (n:Student { name:{nameA} })-[r:relName]->(m:Student { name:{nameB} }) RETURN r.relAttr AS relValue",params);

        while(result.hasNext()){

            Record record = result.next();

            System.out.println(record.get("relValue").asString());
        }

        session.close();
        driver.close();

    }

    @Override
    public void searchOtherNode(Driver driver, String node) {
        Session session = driver.session();

        StatementResult result = session.run("MATCH (n:Student {name:{name}})-[r:relName]->(m:Student) RETURN DISTINCT m.name As nodeName", Values.parameters("name",node));

        while(result.hasNext()){

            Record record = result.next();

            System.out.println(record.get("nodeName").asString());
        }

        session.close();
        driver.close();

    }
}
