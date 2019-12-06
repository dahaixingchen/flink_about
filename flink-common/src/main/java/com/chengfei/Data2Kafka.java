package com.chengfei;

import com.chengfei.kafka.MyKafKaProducer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.log4j.Logger;

import java.io.*;


/**
 * @ClassName: Data2Kafka
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/5 19:11
 * @Version 1.0
 **/
public class Data2Kafka {
    private final static Logger logger = Logger.getLogger(Data2Kafka.class);
    public static void main(String[] args) throws IOException {

        String topic = "flink-cfsdb.tbl_km_trace";
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("C:\\Users\\feifei\\Desktop\\testData\\flinkTest.txt")));
        String data = null;
        MyKafKaProducer myKafKaProducer = new MyKafKaProducer("node-1:9092");
        Producer kafkaProducer = myKafKaProducer.getKafkaProducer();

        while ((data = bufferedReader.readLine()) != null){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, data);
            kafkaProducer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    logger.info("offset" + metadata.offset()+"----partition" + metadata.partition());
                }
            });
        }
    }
}
