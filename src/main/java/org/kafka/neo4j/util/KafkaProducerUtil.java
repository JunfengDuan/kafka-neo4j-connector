package org.kafka.neo4j.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import java.util.Properties;

/**
 * Created by jfd on 4/5/17.
 */
public class KafkaProducerUtil {

    private static Producer<String, String> producer;
    private static final Properties props = new Properties();

    public static Producer getProducer() {

        if(producer == null){
            props.put("bootstrap.servers", "192.168.1.151:9092");
//            props.put("bootstrap.servers", "127.0.0.1:9092");
//            props.put("bootstrap.servers", "centos.master:9092,centos.slave1:9092,centos.slave2:9092");
//            props.put("compression.type", "gzip");
            props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);//32M
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer(props);
        }
        return producer;
    }

}
