package com.chengfei.dynamicTable;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @ClassName: RetractStream
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/28 10:48
 * @Version 1.0
 **/
public class RetractStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        StreamTableEnvironment tabStreamEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        DataStreamSource<Tuple2<String, String>> soData = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Bob", "./prod?id=3")
        );
        Table soTab = tabStreamEnv.fromDataStream(soData, "user,url");
        tabStreamEnv.registerTable("clicks",soTab);
        Table selectTab = tabStreamEnv.sqlQuery("select user,count(1) from clicks group by user");
//        Table selectTab = tabStreamEnv.sqlQuery("select user,count(url) from clicks group by user");

//        DataStream ds = tEnv.toRetractStream(rTable, TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));

        DataStream<Tuple2<Boolean, Tuple2<String, Long>>> selectStream = tabStreamEnv.toRetractStream(selectTab
                , TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));
        selectStream.print();
        tabStreamEnv.execute("retract-stream");


    }
}
