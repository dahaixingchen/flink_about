package com.chengfei.dynamicTable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @ClassName: AppendNolyStream
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/28 9:18
 * @Version 1.0
 * append类型的流是不能group操作的
 **/
public class AppendNolyStream {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnvStream = StreamTableEnvironment.create(env, settings);

        DataStream<Tuple2<String, String>> soData = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Bob", "./prod?id=3")
        );
        Table soTable = tabEnvStream.fromDataStream(soData, "user,url");
        tabEnvStream.registerTable("clicks",soTable);
//        Table selectTable = tabEnvStream.sqlQuery("select * from clicks where user='Mary'");
        Table selectTable = tabEnvStream.sqlQuery("select * from clicks where user='Mary' order by url");
        DataStream<Tuple2<String, String>> appendStream =
                tabEnvStream.toAppendStream(selectTable, TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));
        appendStream.print();
        env.execute("append-onely");


    }
}
