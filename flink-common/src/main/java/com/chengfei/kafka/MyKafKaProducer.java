package com.chengfei.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ClassName: MyKafKaProducer
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/5 19:15
 * @Version 1.0
 **/
public class MyKafKaProducer {
    private final Properties properties = new Properties();
    private String brokers;

    public MyKafKaProducer(String brokers) {
        this.brokers = brokers;
    }

    public Producer getKafkaProducer(){

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,this.brokers);
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        properties.put(ProducerConfig.RETRIES_CONFIG,5);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,2000);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);
        //默认是32M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,33554432);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,3000);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer(properties);
    }
}
