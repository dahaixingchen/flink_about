package com.wsxd.cfs.km.flink.consumeCSVData;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @ClassName: ReadCSVFileTable
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/3 13:55
 * @Version 1.0
 **/
public class ReadCSVFileTable {
    public static void main(String[] args) throws Exception {
        TableEnvironment tabEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());
        Table soTab = tabEnv.fromTableSource(new MyTableSource());
        tabEnv.registerTable("trace",soTab);
        Table groupTab = tabEnv.sqlQuery("select id from trace group by id order by id ");

        tabEnv.registerTableSink("group",new MyTableSink(groupTab.getSchema()));
        groupTab.printSchema();

        tabEnv.execute("d");
    }
}
