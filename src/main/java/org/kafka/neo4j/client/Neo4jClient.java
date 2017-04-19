package org.kafka.neo4j.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.kafka.neo4j.service.Neo4jHandler;
import org.kafka.neo4j.service.Neo4jHandlerImpl;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class Neo4jClient {

    @org.springframework.beans.factory.annotation.Value("${username:neo4j}")
    private String username;
    @org.springframework.beans.factory.annotation.Value("${password:neo4j}")
    private String password;

    private static final Logger logger = LoggerFactory.getLogger(Neo4jClient.class);
    private static Driver driver;

    private Neo4jHandler neo4jHandler;
    private static final String UNIQUE_STRING = "CREATE CONSTRAINT ON (d:%s) ASSERT d.%s IS UNIQUE";
    private static final String INDEX_STRING = "CREATE INDEX ON :%s(%s)";

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

        Map<String,Object> params = decode(message);

        String cypher = createNodeCypher(tag, params);

        StatementResult result = neo4jHandler.createNode(session, cypher, params);
        result.list().forEach(record -> record.values().forEach(value -> logger.info(value.toString())));
        session.close();

    }

    /**
     * CREATE (a:Person {     name: {name}, title: {title}     })
     * @param tag
     * @param params parameters( "name", "Arthur", "title", "King" )
     * @return
     */
    private String createNodeCypher(String tag, Map<String,Object> params){
        StringBuilder builder = new StringBuilder();

        List<String> list = params.keySet().stream().map(key -> key+": {"+key+"}").collect(Collectors.toList());
        String str  = StringUtils.join(list, ',');

        builder.append("CREATE (a:");
        builder.append(tag);
        builder.append(" {");
        builder.append(str);
        builder.append(" })");

        logger.info("Create node cypher-{}",builder.toString());
        return builder.toString();
    }

    /**
     * cypher of creating relationship between two nodes
     *
     * MATCH (cadre:Cadre {cadreID: row.RECORDID})
       MATCH (position:Position {positionID: row.RS0501})
       MERGE (cadre)-[act:ACTAS]->(position)
       ON CREATE SET act.institutionName=row.RS0502, act.area=row.RS0503
     * @param tag
     * @param params
     * @return
     */
    private String createRelationCypher(String tag, Map<String,Object> params){
        return "";
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
        jsonObject.entrySet().forEach(entry -> params.put(entry.getKey(),entry.getValue()));
        return params;
    }

    @PreDestroy
    private void cleanUp(){
        logger.info("Is closing neo4j driver...");
        if(driver != null)
            driver.close();
    }
}
