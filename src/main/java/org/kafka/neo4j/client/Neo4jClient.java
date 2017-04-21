package org.kafka.neo4j.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.kafka.neo4j.service.Neo4jHandler;
import org.kafka.neo4j.service.Neo4jHandlerImpl;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.kafka.neo4j.util.Cypher.*;

/**
 * {"create":{"":""}}
 * {"update":{"":""}}
 * {"delete":{"":""}}
 */
@Component
public class Neo4jClient {

    private static final Logger logger = LoggerFactory.getLogger(Neo4jClient.class);
    @org.springframework.beans.factory.annotation.Value("${username:neo4j}")
    private String username;
    @org.springframework.beans.factory.annotation.Value("${password:neo4j}")
    private String password;
    private static Driver driver;
    private Neo4jHandler neo4jHandler;
    private AtomicInteger retry = new AtomicInteger(0);


    @PostConstruct
    public void init(){

        logger.info("Starting init neo4j...");
//        Driver driver = GraphDatabase.driver( "bolt+routing://localhost", AuthTokens.basic("neo4j", "neo4j") );
        driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( username, password ) );
        logger.info("Init driver-{}",driver);

    }

    public void makeGraph(String tag, String message){
        logger.info("Starting make graph...");
        Session session = driver.session();
        neo4jHandler = new Neo4jHandlerImpl();
//        StatementResult result = null;

        Map<String,Object> params = decode(message);
        String operate = operate(params);
        try {
            if(CREATE_NODE.equals(operate)){
                String nodeCypher = createNodeCypher(tag, params);
                neo4jHandler.createNode(session, nodeCypher, params);
            }else {
                String relationCypher = createRelationCypher(tag, params);
                neo4jHandler.createNodeRelation(session, relationCypher);
            }
            logger.info("Successfully created :{}",message);
        } catch (Exception e) {
            logger.error("{} failed, exception :{}",operate, e.getMessage());
        } finally {
            session.close();
        }
//        neo4jHandler.createIndexOrUniqueConstraint(session, createIndexCypher(tag, ID), createUniqueConstCypher(tag, ID));
    }

    /**
     * CREATE (a:Person {  name: {name}, title: {title}  })
     * @param tag
     * @param params parameters( "name", "Arthur", "title", "King" )
     * @return
     */
    private String createNodeCypher(String tag, Map<String,Object> params){

        List<String> list = params.keySet().stream().map(key -> key+": {"+key+"}").collect(Collectors.toList());
        String props  = StringUtils.join(list, ',');
        String createNode = String.format(CREATE_NODE_STRING, tag, props);

        logger.info("Create node cypher-{}",createNode);
        return createNode;
    }

    /**
     * cypher of creating relationship between two nodes
     *
     * MATCH (cust:Customer {id:''} ),(cc:CreditCard {id:''} )
     * CREATE (cust)-[r:DO_SHOPPING_WITH{shopdate:"12/12/2014",price:55000}]->(cc)
     * @param relationName
     * @param params
     * @return
     */
    private String createRelationCypher(String relationName, Map<String,Object> params){

        List<String> list = params.entrySet().stream().filter(e ->
            !e.getKey().equals(SOURCE) && !e.getKey().equals(TARGET) && !e.getKey().equals(SOURCEID) && !e.getKey().equals(TARGETID)
        ).map(entry -> entry.getKey()+":'"+entry.getValue()+"'").collect(Collectors.toList());

        String rel = StringUtils.join(list, ',');
        String tag1 = (String) params.get(SOURCE) == null ? "" : (String) ((String) params.get(SOURCE)).toLowerCase();
        String sourceId = (String) params.get(SOURCEID);
        String tag2 = (String) params.get(TARGET) == null ? "" : (String) ((String) params.get(TARGET)).toLowerCase();
        String targetId = (String) params.get(TARGETID);
        String match = String.format(MATCH_RELATION_STRING, tag1, ID, sourceId, tag2, ID, targetId);
        String createRel = match+" "+String.format(RELATION_STRING, relationName, rel);

        logger.info("Create relation cypher-{}",createRel);
        return createRel;
    }

    /**
     * Create index for entity
     * "CREATE INDEX ON :Cadre(cadreID)"
     * @param tag The entity which will be indexed
     * @param indexField The index field
     * @return
     */
    private String createIndexCypher(String tag, String indexField){
        return String.format(INDEX_STRING,tag,indexField);
    }

    /**
     * Create unique constraint
     * @param tag
     * @param uniqueField
     * @return
     */
    private String createUniqueConstCypher(String tag, String uniqueField){
        return String.format(UNIQUE_STRING,tag,uniqueField);
    }

    private Map<String,Object> decode(String message) {
        Map<String,Object> params = new HashMap<>();
        JSONObject jsonObject = JSON.parseObject(message);
        jsonObject.entrySet().forEach(entry -> params.put(entry.getKey().toLowerCase(),entry.getValue()));
        return params;
    }

    /**
     * MATCH (n:andres { name: 'Andres' })
       SET n = {};

       MATCH (peter { name: 'Peter' })
       SET peter += { hungry: TRUE , position: 'Entrepreneur' }
     * @return
     */
    private Map updateNodeCypher(String tag, Map<String,Object> params){
        Map<String, String> cypher = new HashMap<>();
        String remove = String.format(REMOVE_STRING, tag, ID, params.get(ID));

        List list = params.entrySet().stream().map(entry -> entry.getKey()+":"+entry.getValue()).collect(Collectors.toList());
        String props = StringUtils.join(list, ',');
        String set = String.format(SET_STRING, tag, ID, params.get(ID), tag, props);
        cypher.put(REMOVE, remove);
        cypher.put(SET, set);
        return cypher;
    }

    private String operate(Map<String, Object> paras){
        Map<String, Object> params = new HashMap<>();
        paras.entrySet().forEach(entry -> params.put(entry.getKey().toLowerCase(),entry.getValue()));
        boolean sourceFlag = params.containsKey(SOURCE) && StringUtils.isNotBlank((String) params.get(SOURCE));
        boolean targetFlag = params.containsKey(TARGET) && StringUtils.isNotBlank((String) params.get(TARGET));
        boolean sourceIdFlag = params.containsKey(SOURCEID) && StringUtils.isNotBlank((String) params.get(SOURCEID));
        boolean targetIdFlag = params.containsKey(TARGETID) && StringUtils.isNotBlank((String) params.get(TARGETID));
        boolean flag = sourceFlag && targetFlag && sourceIdFlag && targetIdFlag;
        if(flag){
            return CREATE_RELATIONSHIP;
        }else{
            return CREATE_NODE;
        }
    }

    @PreDestroy
    private void cleanUp(){
        logger.info("Is closing neo4j driver...");
        if(driver != null)
            driver.close();
    }
}
