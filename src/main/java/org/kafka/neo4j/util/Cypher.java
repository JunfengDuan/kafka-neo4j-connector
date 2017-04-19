package org.kafka.neo4j.util;

/**
 * Created by jfd on 4/19/17.
 */
public interface Cypher {

    String ID = "RECORDID";
    String REMOVE = "REMOVE";
    String SET = "SET";

    /**
     * tag,
     * properties.
     */
    String CREATE_NODE_STRING = "CREATE (:%s { %s })";

    /**
     * tag,
     * index field.
     */
    String INDEX_STRING = "CREATE INDEX ON :%s (%s)";

    /**
     * tag,
     * tag field.
     */
    String UNIQUE_STRING = "CREATE CONSTRAINT ON (d:%s) ASSERT d.%s IS UNIQUE";


    /**
     * tag1
     * tag2
     */
    String MATCH_STRING = "MATCH (node1:%s),(node2:%s)";

    /**
     * relation name,
     * relation properties
     */
    String RELATION_STRING = "CREATE (node1)-[r:%s {%s} ]->(node2)";

    /**
     * tag,
     * index field,
     * field value
     */
    String REMOVE_STRING = "MATCH (:%s { %s: %s }) SET n = {};";

    /**
     * tag
     * index field,
     * field value,
     * tag,
     * properties
     */
    String SET_STRING = "MATCH (%s { %s: %s }) SET %s += { %s }";


}
