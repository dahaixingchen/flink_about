package com.chengfei.dynamicTable;

import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import scala.collection.parallel.ParIterableLike;

/**
 * @ClassName: UpsertStream
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/28 13:50
 * @Version 1.0
 **/
public class UpsertStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabStrEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        DataStreamSource<Tuple2<String, String>> soData = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Liz", "./prod?id=3"),
                new Tuple2<>("Mary", "./prod?id=7")
        );
        Table soTab = tabStrEnv.fromDataStream(soData,"user,url");
        tabStrEnv.registerTable("clicks",soTab);
        Table selectTab = tabStrEnv.sqlQuery("select user,count(url) from clicks group by user");

        tabStrEnv.registerTableSink("selectSinkTab",new MyUpsertSink(selectTab.getSchema()));
        selectTab.insertInto("selectSinkTab");

        tabStrEnv.execute("upsertStream");
    }
}
