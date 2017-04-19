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

    // 创建节点
    StatementResult createNode(Session session, String cypher, Map<String, Object> params);

    // 删除节点
    void deleteNode(Driver driver, Map<String, Object> params);

    // 得到所有节点
    void getAllNode(Driver driver);

    // 创建节点关系
    StatementResult createNodeRelation(Session session, String cypher, Map<String, Object> params);

    // 删除节点关系
    void deleteNodeRelation(Driver driver, Map<String, Object> params);

    // 查询节点所有关系属性
    void searchAllNodeRelation(Driver driver, String relabel);

    // 查询两个节点的关系属性
    void searchBetweenNodeRelation(Driver driver, Map<String, Object> params);

    // 查询此节点关联的其它节点
    void searchOtherNode(Driver driver, String node);

    //add unique constraint
    void createIndexOrUniqueConstraint(Session session, String cypher);

}
