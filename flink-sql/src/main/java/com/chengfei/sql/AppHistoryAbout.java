package com.chengfei.sql;

import com.chengfei.pojo.AppHistory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;
import scala.Tuple1;

/**
 * @ClassName: AppHistoryAbout
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/21 14:44
 * @Version 1.0
 **/
public class AppHistoryAbout {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

//        DataSource<String> textData = env.readTextFile("C:\\Users\\feifei\\Desktop\\kafkaTest.txt");
//        DataSource<String> textData = env.readTextFile("C:\\Users\\feifei\\Desktop\\flinkSqlTest.txt");
        DataSource<String> textData = env.readTextFile("hdfs://node-1:8020/data/flinkSqlTest.txt");
//        DataSource<String> textData = env.readTextFile("hdfs://node-1:8020/data/kafkaTest.txt");

//        DataSource<AppHistory> textData = env.readCsvFile("C:\\Users\\feifei\\Desktop\\kafkaTest.csv")
//                .ignoreFirstLine()
//                .pojoType(AppHistory.class, "id", "app_no", "app_no_cop", "node_name", "rtf_state", "operator_id", "create_time", "claim_time", "remark", "data_date", "SYSTEM_SOURCE");
        DataSet<AppHistory> mapData = textData.flatMap(new FlatMapFunction<String, AppHistory>() {
            @Override
            public void flatMap(String value, Collector<AppHistory> out) throws Exception {
                String[] split = value.split(";");
                AppHistory appHistory = new AppHistory();
                appHistory.setId(split[0]);
                appHistory.setApp_no(split[1]);
                appHistory.setApp_no_cop(split[2]);
                appHistory.setNode_name(split[3]);
                appHistory.setRtf_state(split[4]);
                appHistory.setOperator_id(split[5]);
                appHistory.setCreate_time(split[6]);
                appHistory.setClaim_time(split[7]);
                appHistory.setRemark(split[8]);
                appHistory.setData_date(split[9]);
                appHistory.setSYSTEM_SOURCE(split[10]);
                out.collect(appHistory);
            }
        });

        Table appHistory = tableEnv.fromDataSet(mapData);

        tableEnv.registerTable("appHistory",appHistory);

        Table selectTable = tableEnv.sqlQuery("select app_no from appHistory where operator_id = 'jiangfan' group by app_no order by app_no limit 10");

        DataSet<AppHistory1> appHistoryDataSet = tableEnv.toDataSet(selectTable, AppHistory1.class);


        MapOperator<AppHistory1, Tuple1<String>> map = appHistoryDataSet.map(new MapFunction<AppHistory1, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(AppHistory1 value) throws Exception {
                return new Tuple1<>(value.app_no);
            }
        });
//        map.writeAsText("C:\\Users\\feifei\\Desktop\\flinkSqlTestOut.txt", FileSystem.WriteMode.OVERWRITE ).setParallelism(1);
        map.writeAsText("hdfs://node-1:8020/data/flinkSqlTestOut.txt", FileSystem.WriteMode.OVERWRITE ).setParallelism(1);
        map.print();
//        env.execute("batchto");

    }

    public static  class AppHistory1 {
        public String app_no;
        public AppHistory1(){
            super();
        }


    }
}
