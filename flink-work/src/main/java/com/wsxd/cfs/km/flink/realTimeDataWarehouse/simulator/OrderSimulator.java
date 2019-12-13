package com.wsxd.cfs.km.flink.realTimeDataWarehouse.simulator;

import com.wsxd.cfs.km.flink.realTimeDataWarehouse.config.GlobalConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: OrderSimulator
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/11 10:26
 * @Version 1.0
 **/
public class OrderSimulator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Row> streamSource = streamEnv.addSource(new OrderSource());

//        JDBCTableSource.builder().
        JDBCAppendTableSink tableSink = new JDBCAppendTableSinkBuilder()
                .setDBUrl(GlobalConfig.DB_URL)
                .setDrivername(GlobalConfig.DRIVER_CLASS)
                .setUsername(GlobalConfig.USER_MAME)
                .setPassword(GlobalConfig.PASSWORD)
                .setBatchSize(GlobalConfig.BATCH_SIZE)
                .setQuery("insert into orders (orderNo,userId ,goodId ,goodsMoney ,realTotalMoney ,payFrom ,province) values (?,?,?,?,?,?,?)")
                .setParameterTypes(new TypeInformation[]{
                        Types.STRING, Types.INT, Types.INT, Types.BIG_DEC, Types.BIG_DEC, Types.INT, Types.STRING
//                        BasicTypeInfo.STRING_TYPE_INFO,
//                        BasicTypeInfo.INT_TYPE_INFO,
//                        BasicTypeInfo.INT_TYPE_INFO,
//                        BasicTypeInfo.BIG_DEC_TYPE_INFO,
//                        BasicTypeInfo.BIG_DEC_TYPE_INFO,
//                        BasicTypeInfo.INT_TYPE_INFO,
//                        BasicTypeInfo.STRING_TYPE_INFO
                }).build();
//        JDBCAppendTableSink tableSink = JDBCAppendTableSink.builder()
//                .setDBUrl(GlobalConfig.DB_URL)
//                .setDrivername(GlobalConfig.DRIVER_CLASS)
//                .setUsername(GlobalConfig.USER_MAME)
//                .setPassword(GlobalConfig.PASSWORD)
//                .setBatchSize(GlobalConfig.BATCH_SIZE)
//                .setQuery("insert into orders (orderNo,userId ,goodId ,goodsMoney ,realTotalMoney ,payFrom ,province) values (?,?,?,?,?,?,?)")
//                .setParameterTypes(new TypeInformation[]{
//                        Types.STRING, Types.INT, Types.INT, Types.BIG_DEC, Types.BIG_DEC, Types.INT, Types.STRING
////                        BasicTypeInfo.STRING_TYPE_INFO,
////                        BasicTypeInfo.INT_TYPE_INFO,
////                        BasicTypeInfo.INT_TYPE_INFO,
////                        BasicTypeInfo.BIG_DEC_TYPE_INFO,
////                        BasicTypeInfo.BIG_DEC_TYPE_INFO,
////                        BasicTypeInfo.INT_TYPE_INFO,
////                        BasicTypeInfo.STRING_TYPE_INFO
//                })
//                .build();
        tableSink.emitDataStream(streamSource);

        streamEnv.execute("OrderSimulator");
    }
}
