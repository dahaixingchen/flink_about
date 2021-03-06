package com.chengfei.kafka2mysql;

import com.chengfei.customSink.MySinkToMySQL;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.Properties;


/**
 * @ClassName: Kafka2Mysql
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/18 18:38
 * @Version 1.0
 **/
public class Kafka2Mysql {

    private static Logger logger = Logger.getLogger(Kafka2Mysql.class);
    public static void main(String[] args) {

        Properties pro = new Properties();
        pro.put("bootstrap.servers", "node-1:9092");
        pro.put("group.id", "appHistoryConsumer");
        pro.put("enable.auto.commit", "true"); //自动提交offer
        pro.put("auto.commit.interval.ms", "1000");
        pro.put("session.timeout.ms", "30000");
        pro.put("auto.offset.reset", "earliest");
        pro.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pro.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        pro.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer011<String> flinkConsumer = new FlinkKafkaConsumer011<String>("app_history", new SimpleStringSchema(), pro);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> kafkaData = env.addSource(flinkConsumer);

        DataStream<Tuple3<String,String, Long>> flatMapData = kafkaData.flatMap(new FlatMapFunction<String, Tuple3<String,String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple3<String,String, Long>> out) throws Exception {
                String[] splitStr = s.split(";");
                out.collect(new Tuple3<String, String, Long>(splitStr[1],s, 1L));
            }
        });
        SingleOutputStreamOperator<Tuple3<String, String, Long>> reduceData = flatMapData
                .keyBy(0)
                .timeWindow(Time.seconds(60))
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> value1, Tuple3<String, String, Long> value2) throws Exception {
                return new Tuple3<String, String, Long>(value1.f0, value1.f1, value1.f2 + value2.f2);
             }
        });
        reduceData.print();
        reduceData.addSink(new MySinkToMySQL());
        try {
            env.execute("kafka data to mysql");
        } catch (Exception e) {
            logger.error("程序发生错误"+e);
        }
    }
}
