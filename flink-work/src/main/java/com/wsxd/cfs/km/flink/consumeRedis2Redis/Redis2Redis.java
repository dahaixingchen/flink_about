package com.wsxd.cfs.km.flink.consumeRedis2Redis;

import com.chengfei.pojo.TblKmTrace;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

/**
 * @ClassName: Redis2Redis
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/30 14:04
 * @Version 1.0
 **/
public class Redis2Redis {
    private final static Logger logger = Logger.getLogger(Redis2Redis.class);

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
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> soData = env.addSource(new MyRedisSource());
        DataStream<TblKmTrace> mapData = soData.flatMap(new RichFlatMapFunction<String, TblKmTrace>() {

            private IntCounter numLines = new IntCounter();
            private IntCounter totalDataSize = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", numLines);
                getRuntimeContext().addAccumulator("totalDataSize",totalDataSize);
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

        Table soTab = tabEnv.fromDataStream(mapData, "sys_date,merchantno,saledate,shop,id,name,qty,amount,refundqty,refundamt,create_time,update_time");

        tabEnv.registerTable("TblKmTrace",soTab);

        String sql = "   SELECT *," +
                "       ROW_NUMBER() OVER (PARTITION BY sys_date ORDER BY shop DESC) as row_num" +
                "   FROM TblKmTrace" ;
        sql = "select id from TblKmTrace group by id order by id desc";
//        sql = "select * from TblKmTrace";
        Table selTab = tabEnv.sqlQuery(sql);

        DataStream<Row> resultStream = tabEnv.toRetractStream(selTab, TypeInformation.of(new TypeHint<Row>() {
        })).map(new MapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public Row map(Tuple2<Boolean, Row> value) throws Exception {
                return value.f1;
            }
        });

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost(redisHost)
                .setPort(redisPort)
                .setTimeout(60000)
                .build();
        resultStream.addSink(new RedisSink<>(conf,new MyredisMapper()));

        JobExecutionResult jobResult = env.execute("redis to redis");
        int accumulatorResultNum = jobResult.getAccumulatorResult("num-lines");
        double totalDataSize = (int)(jobResult.getAccumulatorResult("totalDataSize"))/1024.0/1024.0;
        logger.info("共处理数据：" + accumulatorResultNum + " 条");
        logger.info("共处理的数据量：" + String.format("%.2f",totalDataSize) + "M");
    }
}
