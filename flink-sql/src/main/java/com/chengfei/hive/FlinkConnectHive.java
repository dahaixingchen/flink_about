package com.chengfei.hive;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @ClassName: FlinkConnectHIve
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/9 13:34
 * @Version 1.0
 **/
public class FlinkConnectHive {
    public static void main(String[] args) throws Exception {
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(env);

//        DataSource<Tuple2<String, Double>> dataSource = env.fromElements(
//                new Tuple2<String, Double>("Tom", 4.27),
//                new Tuple2<String, Double>("Bob", 8.6),
//                new Tuple2<String, Double>("Tom", 8.0)
//        );
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment batchTableEnv = TableEnvironment.create(build);

        String name = "myhive1";
        String defaultDatabase = "default";
        String hiveConfDir = "/opt/apache-hive-1.2.1-bin/conf";
        String version = "1.2.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);

        batchTableEnv.registerCatalog(name,hiveCatalog);
        batchTableEnv.useCatalog(name);
        batchTableEnv.sqlUpdate("insert into src values ('Tom', 4.27)");
        batchTableEnv.execute("insert into ");
    }
}
