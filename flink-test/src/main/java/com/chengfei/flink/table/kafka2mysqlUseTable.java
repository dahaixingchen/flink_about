package com.chengfei.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.nio.channels.SelectableChannel;

/**
 * @ClassName: kafka2mysqlUseTable
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/27 15:19
 * @Version 1.0
 **/
public class kafka2mysqlUseTable {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        env.setParallelism(1);
        tableEnv.connect(
                new Kafka().version("0.10").topic("user_behavior").property("bootstrap.servers", "node-1:9092").startFromEarliest()
        ).withFormat(
                new Json().deriveSchema()
        )
                .withSchema(
                        new Schema()
                                .field("user_id", Types.STRING)
                                .field("item_id", Types.STRING)
                                .field("category_id", Types.STRING)
                                .field("behavior", Types.STRING)
                                .field("ts", Types.SQL_TIMESTAMP)
                )
                .registerTableSource("user_log");
        String selectSql = "SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:00') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT user_id) AS uv\n" +
                "FROM user_log\n" +
                "GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:00')";
        Table table = tableEnv.sqlQuery(selectSql);
        tableEnv.toDataSet(table,TypeInformation.of(Row.class)).print();

//        tableEnv.toAppendStream(table, TypeInformation.of(Row.class)).print();
//        JDBCAppendTableSink sink = new JDBCAppendTableSinkBuilder()
//                .setDBUrl("jdbc:mysql://192.168.91.4:3306/flink")
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setUsername("chengf")
//                .setPassword("chengf&^y34")
//                .setQuery("insert INTO pvuv_sink(dt,pv,uv)values(?,?,?)")
//                .setParameterTypes(new TypeInformation[]{Types.SQL_TIMESTAMP, Types.LONG, Types.LONG})
//                .build();
//        tableEnv.registerTableSink("Result"
//                ,new String[]{"dt","pv","uv"}
//                ,new TypeInformation[]{Types.SQL_TIMESTAMP, Types.LONG, Types.LONG}
//                ,sink
//                );
//        table.insertInto("Result");
//
        env.execute("table to ");
    }
}
