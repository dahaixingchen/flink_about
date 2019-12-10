package com.wsxd.cfs.km.flink.consumeKafkaData.redisSink;

import com.chengfei.pojo.TblKmTrace;
import com.wsxd.cfs.km.flink.consumeRedis2Redis.MyredisMapper;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * @ClassName: ConsumeKafkaData
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/6 9:29
 * @Version 1.0
 **/
public class ConsumeKafkaData2Redis {
    private final static Logger logger = Logger.getLogger(ConsumeKafkaData2Redis.class);
    public static void main(String[] args) throws Exception {
        final ParameterTool par = ParameterTool.fromArgs(args);
        String redisHost = "node-1";
        int redisPort = 6379;
        if (par.get("redisHost") == null && par.get("redosPort") == null){
            logger.info("Redis默认的服务器地址：192.168.91.201");
            logger.info("Redis默认的端口号：6379");
            logger.info("如要修改可在程序后加上你需要加的配置");
        }else if (par.get("redisHost") != null){
            redisHost = par.get("redisHost");
            logger.info("Redis服务器地址：" + par.get("redisHost"));
            logger.info("Redis默认的端口号：6379");
        }else if (par.get("redosPort") != null){
            redisPort = par.getInt("redosPort");
            logger.info("Redis默认的服务器地址：192.168.91.201");
            logger.info("Redis的端口号：" + par.get("redosPort"));
        }else {
            redisHost = par.get("redisHost");
            redisPort = par.getInt("redosPort");
            logger.info("Redis服务器地址：" + par.get("redisHost"));
            logger.info("Redis的端口号：" + par.get("redosPort"));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        //开启checkpo
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);


        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node-1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>("flink-cfsdb.tbl_km_trace", new SimpleStringSchema(), properties);

        DataStreamSource<String> sourceData = env.addSource(kafkaConsumer);
        DataStream<TblKmTrace> flatMapData = sourceData.flatMap(new RichFlatMapFunction<String, TblKmTrace>() {

            private IntCounter numLines = new IntCounter();
            private IntCounter totalDataSize = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", numLines);
                getRuntimeContext().addAccumulator("totalDataSize", totalDataSize);
            }

            @Override
            public void flatMap(String value, Collector<TblKmTrace> out) throws Exception {
                String[] splits = value.split(";");
                TblKmTrace tblKmTrace = new TblKmTrace();
                if (splits[0] != null) {
                    tblKmTrace.setSys_date(splits[0]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[1] != null) {
                    tblKmTrace.setMerchantno(splits[1]);
                } else {
                    tblKmTrace.setMerchantno("");
                }
                if (splits[2] != null) {
                    tblKmTrace.setSaledate(splits[2]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[3] != null) {
                    tblKmTrace.setShop(splits[3]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[4] != null) {
                    tblKmTrace.setId(splits[4]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[5] != null) {
                    tblKmTrace.setName(splits[5]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[6] != null) {
                    tblKmTrace.setQty(splits[6]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[7] != null) {
                    tblKmTrace.setAmount(splits[7]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[8] != null) {
                    tblKmTrace.setRefundqty(splits[8]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[9] != null) {
                    tblKmTrace.setRefundamt(splits[9]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[10] != null) {
                    tblKmTrace.setCreate_time(splits[10]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                if (splits[11] != null) {
                    tblKmTrace.setUpdate_time(splits[11]);
                } else {
                    tblKmTrace.setSys_date("");
                }
                out.collect(tblKmTrace);
                numLines.add(1);
                totalDataSize.add(value.length());
                Thread.sleep(5);
            }
        });
        Table sourceTable = tableEnv.fromDataStream(flatMapData,"sys_date,merchantno,saledate,shop,id,name,qty,amount,refundqty,refundamt,create_time,update_time");
        tableEnv.registerTable("TblKmTrace",sourceTable);
        Table resultTable = tableEnv.sqlQuery("select id from TblKmTrace group by id ");

        DataStream<Row> resultData = tableEnv.toRetractStream(resultTable, TypeInformation.of(new TypeHint<Row>() {
        })).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> value) throws Exception {
                return value.f1;
            }
        });
//        resultData.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHost)
                .setPort(redisPort)
                .setTimeout(60000)
                .build();
        resultData.addSink(new RedisSink<>(conf,new MyredisMapper()));

        env.execute("data to kafka");
    }
}
