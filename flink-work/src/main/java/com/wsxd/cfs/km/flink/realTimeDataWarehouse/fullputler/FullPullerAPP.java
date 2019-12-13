package com.wsxd.cfs.km.flink.realTimeDataWarehouse.fullputler;

import com.wsxd.cfs.km.flink.realTimeDataWarehouse.config.GlobalConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import javax.activation.MailcapCommandMap;

/**
 * @ClassName: FullPullerAPP
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/12 10:21
 * @Version 1.0
 **/
public class FullPullerAPP {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        JDBCInputFormat inputFormat = new JDBCInputFormat.JDBCInputFormatBuilder()
                .setDrivername(GlobalConfig.DRIVER_CLASS)
                .setDBUrl(GlobalConfig.DB_URL)
                .setUsername(GlobalConfig.USER_MAME)
                .setPassword(GlobalConfig.PASSWORD)
                .setQuery("select * from goods")
                .setRowTypeInfo(new RowTypeInfo(Types.INT, Types.STRING, Types.BIG_DEC, Types.INT, Types.INT))
                .finish();
        DataSource<Row> rowDataSource = env.createInput(inputFormat);

        rowDataSource.print();
        rowDataSource.output(new HbaseOutputFormat("node-1:2181,node-2:2181,node-1:2181","goods","F"));
        env.execute("FullPullerAPP");
    }
}
