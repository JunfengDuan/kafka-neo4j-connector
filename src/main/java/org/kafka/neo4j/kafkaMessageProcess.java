package org.kafka.neo4j;

import org.kafka.neo4j.client.KafkaConsumerClient;
import org.kafka.neo4j.client.Neo4jClient;
import org.kafka.neo4j.service.ProduceMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by jfd on 4/5/17.
 * 
 * 子表携带父表信息：parentName,parentId
 * partition 划分策略
 * 分布式,多线程
 */
/*@Component
public class kafkaMessageProcess implements CommandLineRunner{

    private static final Logger logger = LoggerFactory.getLogger(kafkaMessageProcess.class);

    @Override
    public void run(String... strings) throws Exception {

        logger.info("Sending a message to kafka...");

        ProduceMessageService.send();

    }

}*/

public class kafkaMessageProcess{

    public static void main(String[] args){

//        ProduceMessageService.send();
    }


}
