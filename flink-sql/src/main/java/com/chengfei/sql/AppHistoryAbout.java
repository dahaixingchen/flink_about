package com.chengfei.sql;

import com.chengfei.pojo.AppHistory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.util.Collector;
import scala.Tuple1;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;


/**
 * @ClassName: AppHistoryAbout
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/21 14:44
 * @Version 1.0
 * 当数据量大的时候回出现一个数据越界的bug，解决方案：



 **/
public class AppHistoryAbout {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //可以完整的使用，但是flink读文件的API都不能实现多线程的模式读取
//        DataSource<String> textData = env.readTextFile("C:\\Users\\feifei\\Desktop\\testData\\kafkaTest.txt");
//        DataSource<String> textData = env.readTextFile("C:\\Users\\feifei\\Desktop\testData\\flinkSqlTest.txt");

        //读取文件的方式应该如果用hdfs的读取方式这样就能增加平行度了
        DataSource<String> textData = env.readTextFile("hdfs://node-1:8020/data/flinkSqlTest.txt");
//        DataSource<String> textData = env.readTextFile("hdfs://node-1:8020/data/kafkaTest.txt");

        //CSV格式的数据量大就会产生bug这个问题
//        CsvReader textData = env.readCsvFile("C:\\Users\\feifei\\Desktop\testData\\kafkaTest.csv");
//        CsvReader textData = env.readCsvFile("C:\\Users\\feifei\\Desktop\testData\\lessTest.csv");
//        textData.setCharset("GBK");
//        DataSource<AppHistory> mapData = textData
//                .ignoreFirstLine()
//                .pojoType(AppHistory.class, "id", "app_no", "app_no_cop", "node_name", "rtf_state", "operator_id", "create_time", "claim_time", "remark", "data_date", "SYSTEM_SOURCE");

        DataSet<AppHistory> mapData = textData.flatMap(new FlatMapFunction<String, AppHistory>() {
            @Override
            public void flatMap(String value, Collector<AppHistory> out) throws Exception {

                String[] split = value.split(";");
                AppHistory appHistory = new AppHistory();
                for (int i = 0; i <split.length ;i++){
                    if (i ==0){
                        appHistory.setId(split[0]);
                    }if (i==1){
                        appHistory.setApp_no(split[1]);
                    }else if (i==2){
                        appHistory.setApp_no_cop(split[2]);
                    }else if (i==3){
                        appHistory.setNode_name(split[3]);
                    }else if (i==4){
                        appHistory.setRtf_state(split[4]);
                    }else if (i==5){
                        appHistory.setOperator_id(split[5]);
                    }else if (i==6){
                        appHistory.setCreate_time(split[6]);
                    }else if (i==7){
                        appHistory.setClaim_time(split[7]);
                    }else if (i==8){
                        appHistory.setRemark(split[8]);
                    }else if (i==9){
                        appHistory.setData_date(split[9]);
                    }else if (i==10){
                        appHistory.setSYSTEM_SOURCE(split[10]);;
                    }
                }
                out.collect(appHistory);
            }
        });


        Table appHistory = tableEnv.fromDataSet(mapData);

        tableEnv.registerTable("appHistory",appHistory);

//        Table selectTable = tableEnv.sqlQuery("select app_no from appHistory where operator_id = 'jiangfan' group by app_no order by app_no limit 10");
        Table selectTable = tableEnv.sqlQuery(" select app_no from appHistory where operator_id = 'jiangfan' group by app_no order by app_no ");

        DataSet<AppHistory1> appHistoryDataSet = tableEnv.toDataSet(selectTable, AppHistory1.class);
//        DataSet<AppHistory1> appHistoryDataSet = tableEnv.toDataSet(selectTable, AppHistory1.class);


        MapOperator<AppHistory1, Tuple1<String>> map = appHistoryDataSet.map(new MapFunction<AppHistory1, Tuple1<String>>() {
            @Override
            public Tuple1<String> map(AppHistory1 value) throws Exception {
                return new Tuple1<>(value.app_no);
            }
        });
//        map.writeAsText("C:\\Users\\feifei\\Desktop\\flinkSqlTestOut.txt", FileSystem.WriteMode.OVERWRITE ).setParallelism(1);
//        map.writeAsText("hdfs://node-1:8020/data/flinkSqlTestOut.txt", FileSystem.WriteMode.OVERWRITE ).setParallelism(1);

        //print最后会调用execute这个命令
        map.print();
//        env.execute("batchto");

    }

    //这个类必须要是public的而且是static的
    public static  class AppHistory1 {
        public String app_no;
        public AppHistory1(){
            super();
        }


    }
}
