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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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
    @org.springframework.beans.factory.annotation.Value("${host:localhost}")
    private String host;
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
        driver = GraphDatabase.driver( "bolt://"+host+":7687", AuthTokens.basic( username, password ) );
        logger.info("Init driver-{}",driver);

    }

    public void makeGraph(String label, String message){
        logger.info("Starting make graph...");
        Session session = driver.session();
        neo4jHandler = new Neo4jHandlerImpl();
//        StatementResult result = null;

        Map<String,Object> params = dateConvert(decode(message));
        String operate = operate(params);
        try {
            if(CREATE_NODE.equals(operate)){
                String nodeCypher = createNodeCypher(label, params);
                neo4jHandler.createNode(session, nodeCypher, params);
            }else if(CREATE_RELATIONSHIP.equals(operate)){
                String relationCypher = createRelationCypher(label, params);
                neo4jHandler.createNodeRelation(session, relationCypher);
            }else if(UPDATE_NODE.equals(operate)){
                Map updateNodeCypher = updateNodeCypher(label, params);
                neo4jHandler.updateNode(session, updateNodeCypher);
            }
            logger.info("Successfully {} :{}",operate,message);
        } catch (Exception e) {
            logger.error("{} failed, exception :{}",operate, e.getMessage());
        } finally {
            session.close();
        }
//        neo4jHandler.createIndexOrUniqueConstraint(session, createIndexCypher(label, ID), createUniqueConstCypher(label, ID));
    }

    /**
     * CREATE (a:Person {  name: {name}, title: {title}  })
     * @param label
     * @param params parameters( "name", "Arthur", "title", "King" )
     * @return
     */
    private String createNodeCypher(String label, Map<String,Object> params){

        List<String> list = params.keySet().stream().map(key -> key+": {"+key+"}").collect(Collectors.toList());
        String props  = StringUtils.join(list, ',');
        String createNode = String.format(CREATE_NODE_STRING, label, props);

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
        String label1 =  params.get(SOURCE) == null ? "" : (String) ( params.get(SOURCE));
        String sourceId = (String) params.get(SOURCEID);
        String label2 =  params.get(TARGET) == null ? "" : (String) ( params.get(TARGET));
        String targetId = (String) params.get(TARGETID);
        String match = String.format(MATCH_RELATION_STRING, label1, ID, sourceId, label2, ID, targetId);
        String relName = params.get(RELNAME) == null ? relationName : (String) params.get(RELNAME);
        String createRel = match+" "+String.format(RELATION_STRING, relName, rel);

        logger.info("Create relation cypher-{}",createRel);
        return createRel;
    }

    /**
     * Create index for entity
     * "CREATE INDEX ON :Cadre(cadreID)"
     * @param label The entity which will be indexed
     * @param indexField The index field
     * @return
     */
    private String createIndexCypher(String label, String indexField){
        return String.format(INDEX_STRING,label,indexField);
    }

    /**
     * Create unique constraint
     * @param label
     * @param uniqueField
     * @return
     */
    private String createUniqueConstCypher(String label, String uniqueField){
        return String.format(UNIQUE_STRING,label,uniqueField);
    }

    private Map<String,Object> decode(String message) {
        Map<String,Object> params = new HashMap<>();
        JSONObject jsonObject = JSON.parseObject(message);
        jsonObject.entrySet().forEach(entry -> params.put(entry.getKey().toLowerCase(),entry.getValue()));
        return params;
    }

    private Map<String,Object> dateConvert(Map<String,Object> params){
        Map<String,Object> paras = new HashMap<>();
        params.entrySet().forEach(entry -> {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(key.contains("日期") || key.contains("时间") || key.toLowerCase().contains("date") ||
                    key.toLowerCase().contains("time") ||
                    key.toLowerCase().contains("day") ||  key.toLowerCase().contains("a0154")){
                value = toDate((Long) entry.getValue());
            }
            paras.put(key, value);
        });

        return paras;
    }

    private static String toDate(Long now){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(now);
        return dateFormat.format(calendar.getTime());
    }

    /**
     * MATCH (n:andres { name: 'Andres' })
       SET n = {};

       MATCH (peter { name: 'Peter' })
       SET peter += { hungry: TRUE , position: 'Entrepreneur' }
     * @return
     */
    private Map updateNodeCypher(String label, Map<String,Object> params){
        Map<String, String> cypher = new HashMap<>();
        List list = params.entrySet().stream().filter(e -> !OP.equalsIgnoreCase(e.getKey()) && !ID.equalsIgnoreCase(e.getKey()))
                .map(entry -> entry.getKey()+":'"+entry.getValue()+"'").collect(Collectors.toList());
        String props = StringUtils.join(list, ',');
        String set = String.format(SET_STRING, label, ID, params.get(ID), label, props);
        cypher.put(SET, set);
        return cypher;
    }

    private String operate(Map<String, Object> paras){
        Map<String, Object> params = new HashMap<>();
        paras.entrySet().forEach(entry -> params.put(entry.getKey().toLowerCase(),entry.getValue()));
        boolean op = params.containsKey(OP) && StringUtils.isNotBlank((String) params.get(OP));
        boolean sourceFlag = params.containsKey(SOURCE) && StringUtils.isNotBlank((String) params.get(SOURCE));
        boolean targetFlag = params.containsKey(TARGET) && StringUtils.isNotBlank((String) params.get(TARGET));
        boolean sourceIdFlag = params.containsKey(SOURCEID) && StringUtils.isNotBlank((String) params.get(SOURCEID));
        boolean targetIdFlag = params.containsKey(TARGETID) && StringUtils.isNotBlank((String) params.get(TARGETID));
        boolean flag = sourceFlag || targetFlag || sourceIdFlag || targetIdFlag;
        if(flag){
            if(op && (UPDATE_RELATIONSHIP.equalsIgnoreCase((String)params.get(OP)))){
                return UPDATE_RELATIONSHIP;
            }else{
                return CREATE_RELATIONSHIP;
            }
        }else {
            if(op && (UPDATE_NODE.equalsIgnoreCase((String)params.get(OP)))){
                return UPDATE_NODE;
            }else{
                return CREATE_NODE;
            }
        }
    }

    @PreDestroy
    private void cleanUp(){
        logger.info("Is closing neo4j driver...");
        if(driver != null)
            driver.close();
    }
}
