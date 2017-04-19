package org.kafka.neo4j.client;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kafka.neo4j.util.KafkaProducerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by jfd on 4/6/17.
 */
public class KafkaProducerClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerClient.class);

    private static final String ID = "id";

    private static Producer producer = KafkaProducerUtil.getProducer();

    private RecordMetadata recordMetadata;

    public RecordMetadata sendMessage(String topic, Object obj){

        JSONObject json = (JSONObject) obj;

        logger.info("\n发送到kafka的消息：{}", json);

        ProducerRecord data = new ProducerRecord(topic, getKey(json), json.toString());

        producer.send(data,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        recordMetadata = metadata;
                        if(e != null)
                            e.printStackTrace();
                        logger.info("\nThe offset of the record we just sent is :{},\nThe partition of the record we just sent is :{}" +
                                "\nThe topic of the record we just sent is :{}" , metadata.offset(), metadata.partition(), metadata.topic());
                    }
                });
        return recordMetadata;
    }

    private String getKey(JSONObject json){
        return (String)json.get(ID);
    }

    public void close(){
        if(producer != null){
            producer.flush();
            producer.close();
        }
    }

}
