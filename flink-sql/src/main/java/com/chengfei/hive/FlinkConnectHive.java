package com.chengfei.hive;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveCatalogConfig;
import org.apache.flink.table.planner.sinks.TableSinkUtils;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.TableSinkBase;
import org.apache.flink.types.Row;

/**
 * @ClassName: FlinkConnectHIve
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/9 13:34
 * @Version 1.0
 **/
public class FlinkConnectHive {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment batchTableEnv = TableEnvironment.create(build);


        String name = "myhive1";
        String defaultDatabase = "default";
        String hiveConfDir = "/home/hadoop/hive/conf";
        String version = "2.3.4";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);

        batchTableEnv.registerCatalog(name,hiveCatalog);
        batchTableEnv.useCatalog(name);
//        batchTableEnv.sqlUpdate("insert into stu values('Tom',cast(56.5 as double))");
        Table table = batchTableEnv.sqlQuery("select * from stu");
        batchTableEnv.registerTableSink("stuOnConsole"
                ,new String[]{"name","value"}
                ,new TypeInformation[]{Types.STRING,Types.DOUBLE}
                ,new MyTableSink());
        table.insertInto("");
        batchTableEnv.execute("FlinkConnectHive");
    }
    private static class MyTableSink implements TableSink{

        @Override
        public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
            return null;
        }
    }
}
