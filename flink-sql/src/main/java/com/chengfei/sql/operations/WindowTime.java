package com.chengfei.sql.operations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @ClassName: WindowTime
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/29 14:29
 * @Version 1.0
 **/
public class WindowTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        DataStreamSource<Tuple5<String, String, String, String, String>> soStream = env.fromElements(
                new Tuple5<>("884188", "2331370", "3607361", "pv", "2019-06-06 13:51:36"),
                new Tuple5<>("72929", "762495", "4223012", "fav",  "2019-06-05 03:38:48"),
                new Tuple5<>("266745", "572404", "2462567", "cart","2019-06-05 04:58:35"),
                new Tuple5<>("621179", "4806156", "4145813", "pv", "2019-06-05 05:11:20"),
                new Tuple5<>("666200", "2687350", "1080785", "pv", "2019-06-05 07:00:53"),
                new Tuple5<>("885793", "3023169", "1320293", "pv", "2019-06-05 06:35:46"),
                new Tuple5<>("681372", "4729122", "4756105", "pv", "2019-06-05 07:00:54"),
                new Tuple5<>("304122", "83595", "3780321", "pv",   "2019-06-05 07:01:26"),
                new Tuple5<>("668791", "5020614", "153285 ", "pv", "2019-06-05 07:00:50"),
                new Tuple5<>("268880", "636630", "1464116", "pv",  "2019-06-05 07:00:58"),
                new Tuple5<>("611384", "3286439", "2520771", "pv", "2019-06-05 07:00:55"),
                new Tuple5<>("505801", "4642858", "238434 ", "pv", "2019-06-05 07:01:04"),
                new Tuple5<>("140394", "4967308", "4551297", "pv", "2019-06-05 07:01:05"),
                new Tuple5<>("613765", "4415901", "4339722", "pv", "2019-06-05 07:01:33")

        );
        Table table = tabEnv.fromDataStream(soStream, "user_id,item_id,category_id,behavior,ts");
        tabEnv.registerTable("user_log",table);
        String sql = "SELECT\n" +
                "  DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss') dt,\n" +
                "  COUNT(*) AS pv,\n" +
                "  COUNT(DISTINCT user_id) AS uv\n" +
                "FROM user_log\n" +
                "GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd HH:mm:ss')";
        Table selectTab = tabEnv.sqlQuery(sql);
//        tabEnv.toRetractStream(selectTab, TypeInformation.of(new TypeHint<Tuple3<String,Long,Long>>() {
//        })).print();

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
        selectTab.insertInto("pvuv_sink");

        tabEnv.execute("selfSink and source");
    }
}
