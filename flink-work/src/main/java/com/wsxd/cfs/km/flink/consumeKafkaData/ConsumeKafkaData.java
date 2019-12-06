package com.wsxd.cfs.km.flink.consumeKafkaData;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @ClassName: ConsumeKafkaData
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/6 9:29
 * @Version 1.0
 **/
public class ConsumeKafkaData {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node-1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>("flink-cfsdb.tbl_km_trace", new SimpleStringSchema(), properties);

        DataStreamSource<String> sourceData = env.addSource(kafkaConsumer);
        Table sourceTable = tableEnv.fromDataStream(sourceData);
        tableEnv.registerTable("trace",sourceTable);
        tableEnv.sqlQuery("select id from ")
//        Schema scheam =new Schema()
//                .field("merchantno", Types.STRING)
//                .field("saledate", Types.STRING)
//                .field("shop", Types.STRING)
//                .field("id", Types.STRING)
//                .field("name", Types.STRING)
//                .field("qty", Types.STRING)
//                .field("amount", Types.STRING)
//                .field("refundqty", Types.STRING)
//                .field("refundamt", Types.STRING);
//        tableEnv.connect(
//            new Kafka()
//                .version("universal")
//                .topic("flink-cfsdb.tbl_km_trace")
//                .startFromEarliest()
//                .properties(properties)
//        )
//                .withSchema(scheam)
//                .withFormat()

    }
}
