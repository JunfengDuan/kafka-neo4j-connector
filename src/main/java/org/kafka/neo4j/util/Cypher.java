package org.kafka.neo4j.util;

/**
 * Created by jfd on 4/19/17.
 */
public interface Cypher {

    String ID = "id";
    String SOURCEID = "sourceid";
    String TARGETID = "targetid";
    String REMOVE = "REMOVE";
    String SET = "SET";
    String CREATE_NODE = "create_node";
    String UPDATE_NODE = "update_node";
    String CREATE_RELATIONSHIP = "create_relationship";
    String UPDATE_RELATIONSHIP = "update_relationship";
    String SOURCE = "source";
    String TARGET = "target";
    String OP = "op";
    String RELNAME = "relname";
    /**
     * label,
     * properties.
     */
    String CREATE_NODE_STRING = "CREATE (:%s { %s })";

    /**
     * label,
     * index field.
     */
    String INDEX_STRING = "CREATE INDEX ON :%s (%s)";

    /**
     * label,
     * label field.
     */
    String UNIQUE_STRING = "CREATE CONSTRAINT ON (d:%s) ASSERT d.%s IS UNIQUE";


    /**
     * label1,
     * index,
     * index field,
     * label2,
     * index,
     * index field
     */
    String MATCH_RELATION_STRING = "MATCH (node1:%s {%s:'%s'}),(node2:%s {%s:'%s'})";

    /**
     * relation name,
     * relation properties
     */
    String RELATION_STRING = "CREATE (node1)-[r:%s {%s} ]->(node2)";

    /**
     * label,
     * index field,
     * field value
     */
    String REMOVE_STRING = "MATCH (n:%s { %s: %s }) SET n = {};";

    /**
     * label
     * index field,
     * field value,
     * label,
     * properties
     */
    String SET_STRING = "MATCH (%s { %s: '%s' }) SET %s += { %s }";

    /**
     * label,
     * index field,
     * field value,
     */
    String QUERY_NODE_STRING = "MATCH (n:%s { %s: %s }),  RETURN n";

    /**
     * label1,
     * index field1,
     * field value1,
     * label2,
     * index field2,
     * field value2,
     * relationship
     */
    String QUERY_RELATIONSHIP_STRING = "MATCH (a:%s { %s: %s })\n" +
            "MATCH (b:%s { %s: %s })\n"+
            "OPTIONAL MATCH (a)-[r:%s]->(b)\n" +
            "RETURN r";


}
