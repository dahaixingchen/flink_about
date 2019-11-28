package test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @ClassName: kafka2Mysql
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/26 13:43
 * @Version 1.0
 * 还是用流的方式开发，看下能不能行
 **/
public class kafka2Mysql {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tabEnv = TableEnvironment.create(build);
        String sqlCreate = "CREATE TABLE user_log (\n" +
                "    user_id VARCHAR,\n" +
                "    item_id VARCHAR,\n" +
                "    category_id VARCHAR,\n" +
                "    behavior VARCHAR,\n" +
                "    ts TIMESTAMP\n" +
                ") WITH (\n" +
                "    'connector.type' = 'kafka',\n" +
                "    'connector.version' = 'universal',\n" +
                "    'connector.topic' = 'user_behavior',\n" +
                "    'connector.startup-mode' = 'earliest-offset',\n" +
                "    'connector.properties.0.key' = 'zookeeper.connect',\n" +
                "    'connector.properties.0.value' = 'node-1:2181',\n" +
                "    'connector.properties.1.key' = 'bootstrap.servers',\n" +
                "    'connector.properties.1.value' = 'node-1:9092',\n" +
                "    'update-mode' = 'append',\n" +
                "    'format.type' = 'json',\n" +
                "    'format.derive-schema' = 'true'\n" +
                ")";
        Table table = tabEnv.sqlQuery("select * from pvuv_sink");

        JDBCAppendTableSink sink = new JDBCAppendTableSinkBuilder()
                .setDBUrl("jdbc:mysql://192.168.91.4:3306/flink")
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("chengf")
                .setPassword("chengf&^y34")
                .setQuery("REPLACE INTO pvuv_sink(dt,pv,uv)values(?,?,?)")
                .setParameterTypes(new TypeInformation[]{Types.STRING, Types.BIG_DEC, Types.BIG_INT})
                .build();
        tabEnv.registerTableSink("Result"
                ,new String[]{"dt","pv","uv"}
                ,new TypeInformation[]{Types.STRING, Types.BIG_DEC, Types.BIG_INT}
                ,sink);
        table.insertInto("Result");

        String sqlsink = "CREATE TABLE pvuv_sink (\n" +
                "    dt VARCHAR,\n" +
                "    pv BIGINT,\n" +
                "    uv BIGINT\n" +
                ") WITH (\n" +
                "    'connector.type' = 'jdbc',\n" +
                "    'connector.url' = 'jdbc:mysql://192.168.91.4:3306/flink',\n" +
                "    'connector.table' = 'pvuv_sink',\n" +
                "    'connector.username' = 'chengf',\n" +
                "    'connector.password' = 'chengf&^y34',\n" +
                "    'connector.write.flush.max-rows' = '1'\n" +
                ")";
        tabEnv.sqlUpdate(sqlsink);

        tabEnv.execute("job for sql");

    }
}
