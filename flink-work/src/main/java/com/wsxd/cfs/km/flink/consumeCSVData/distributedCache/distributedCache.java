package com.wsxd.cfs.km.flink.consumeCSVData.distributedCache;

import com.wsxd.cfs.km.flink.pojo.Trace;
import com.wsxd.cfs.km.flink.consumeRedis2Redis.Redis2Redis;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

/**
 * @ClassName: distributedCache
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/3 20:27
 * @Version 1.0
 **/
public class distributedCache {
    private static Logger logger = Logger.getLogger(Redis2Redis.class);
    public static void main(String[] args) throws Exception {
        String redisHost = "node-1";
        int redisPort = 6379;
        if (args.length == 0){
            logger.info("Redis默认的服务器地址：192.168.91.201");
            logger.info("Redis默认的端口号：6379");
            logger.info("如要修改可在程序后加上你需要加的配置");
        }else if (args.length == 1){
            logger.info("Redis服务器地址：" + args[0]);
            logger.info("Redis默认的端口号：6379");
            redisHost = args[0];
        }else if (args.length == 2){
            logger.info("Redis服务器地址：" + args[0]);
            logger.info("Redis默认的端口号：" + args[1]);
            redisHost = args[0];
            redisPort = Integer.valueOf(args[1]);
        }else{
            logger.info("您输入的参数个数有误,请从新输入Redis的服务器地址或是IP");
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tabEnvBat = BatchTableEnvironment.create(env);
        TableEnvironment tabEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build());

//        DataSource<Trace> soData = env.readCsvFile("E:\\WorkData\\xinshen\\TRACE-test.csv")
        DataSource<Trace> soData = env.readCsvFile("/root/xinshen/TRACE-test.csv")
                .ignoreFirstLine()
                .pojoType(Trace.class,
                        "merchantno","saledate","shop","id","name","qty","amount","refundqty","refundamt")
                .setParallelism(10);
        Table soTab = tabEnvBat.fromDataSet(soData);
        tabEnvBat.registerTable("trace",soTab);
        Table selTab = tabEnvBat.sqlQuery("select id from trace group by id order by id ");
//        Table selTab = tabEnvBat.sqlQuery("select * from trace");
        DataSet<Row> resultData = tabEnvBat.toDataSet(selTab, Row.class);
        resultData.print();
//        resultData.output(new MyOutput());

        env.execute("read csv file");
    }
}
