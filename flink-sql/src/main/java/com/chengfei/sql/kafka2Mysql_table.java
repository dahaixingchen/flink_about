package com.chengfei.sql;

import com.mysql.jdbc.JDBC4MySQLConnection;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @ClassName: kafka2Mysql
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/26 13:43
 * @Version 1.0
 * 用table的方式开发kafka数据到mysql
 **/
public class kafka2Mysql_table {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tabEnv = TableEnvironment.create(build);

        tabEnv.connect(
                new Kafka()
                .version("universal")
                .topic("user_behavior")
                .startFromEarliest()
                .property("zookeeper.connect", "node-1:2181")
                .property("bootstrap.servers", "node-1:9092")
        )
                .withFormat(
                        new Json().deriveSchema()
                )
                .withSchema(
                        new Schema()
                                .field("user_id", Types.STRING)
                                .field("item_id", Types.STRING)
                                .field("category_id", Types.STRING)
                                .field("behavior", Types.STRING)
                                .field("ts", Types.STRING)
                )
                .inAppendMode()
                .registerTableSource("user_log");


        JDBCOptions options = JDBCOptions
                .builder()
                .setDBUrl("jdbc:mysql://192.168.91.4:3306/flink")
                .setUsername("chengf")
                .setPassword("chengf&^y34")
                .setTableName("pvuv_sink")
                .build();
        TableSchema schema = TableSchema
                .builder()
                .field("dt", Types.STRING)
                .field("pv", Types.LONG)
                .field("uv", Types.LONG)
                .build();

        JDBCUpsertTableSink sink = JDBCUpsertTableSink
                .builder()
                .setOptions(options)
                .setTableSchema(schema)
                .build();

        tabEnv.registerTableSink("pvuv_sink"
                ,new String[]{"dt","pv","uv"}
                ,new TypeInformation[]{Types.STRING, Types.LONG, Types.LONG}
                ,sink);
//        table.insertInto("Result");
        String insertSql = "INSERT INTO pvuv_sink\n" +
                "SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT user_id) AS uv\n" +
                "FROM user_log\n" +
                "GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm')";
        tabEnv.sqlUpdate(insertSql);
        tabEnv.execute("job for sql");

    }
}
