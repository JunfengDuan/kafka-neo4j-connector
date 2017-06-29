package org.kafka.neo4j.service;

import com.alibaba.fastjson.JSONObject;
import org.kafka.neo4j.client.KafkaProducerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jfd on 4/16/17.
 */
public class ProduceMessageService {

    private static final Logger logger = LoggerFactory.getLogger(ProduceMessageService.class);
    static KafkaProducerClient simpleProducer = null;

    public static void send(){
        logger.info("*******************kafkaMessageProcess is started.**********************");

        simpleProducer= new KafkaProducerClient();
        Map<String, JSONObject> map = getJsonObject2();
        map.entrySet().forEach(entry -> simpleProducer.sendMessage(entry.getKey(),entry.getValue()));

        simpleProducer.close();

        logger.info("*****************Messages are processed successfully ! ********************");
    }

    public static Map getJsonObject2(){
        Map<String, JSONObject> map = new HashMap();
        JSONObject cadreJson = new JSONObject();
        cadreJson.put("id", "222");
        cadreJson.put("url","/zfcg/views/zfcg/zbgs/zfcg_zbgs_info.jsp");
        cadreJson.put("parentTableName", "");
        cadreJson.put("parentId", "");
        cadreJson.put("zbje", "23");

        map.put("per",cadreJson);
        return  map;
    }

    public static Map getJsonObject1(){
        Map<String, JSONObject> map = new HashMap();
        JSONObject cadreJson = new JSONObject();
        cadreJson.put("id", "222");
        cadreJson.put("name","汪峰");
        cadreJson.put("op","Delete");
//        cadreJson.put("parentTableName", "");
//        cadreJson.put("parentId", "");

//        JSONArray eduArray = new JSONArray();
        JSONObject eduJson = new JSONObject();
        eduJson.put("id", UUID.randomUUID().toString());
        eduJson.put("university", "PKU");
        eduJson.put("major", "计算机");
        eduJson.put("parentTableName", "per");
        eduJson.put("parentId", cadreJson.get("id"));

//        eduArray.add(eduJson);
        map.put("per",cadreJson);
//        map.put("edu",eduJson);
        return  map;
    }

    public Map getJsonObject(){
        Map<String, JSONObject> map = new HashMap();
        JSONObject cadreJson = new JSONObject();
        cadreJson.put("cadreId","11");
        cadreJson.put("name","Elena");

        JSONObject eduJson = new JSONObject();
        eduJson.put("eduId", "22");
        eduJson.put("University", "PKU");
        eduJson.put("major", "AI");
        eduJson.put("Cadre",cadreJson);

        map.put("Cadre",cadreJson);
        map.put("Education",eduJson);
        return  map;
    }
}
