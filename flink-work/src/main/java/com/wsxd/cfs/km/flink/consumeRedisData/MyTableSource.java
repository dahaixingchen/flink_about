package com.wsxd.cfs.km.flink.consumeRedisData;

import com.wsxd.cfs.km.flink.pojo.Trace;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.types.DataType;

/**
 * @ClassName: MyTableSource
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/3 13:56
 * @Version 1.0
 **/
public class MyTableSource implements BatchTableSource<Trace> {

    @Override
    public DataSet<Trace> getDataSet(ExecutionEnvironment execEnv) {
        DataSource<Trace> traceDataSource = execEnv.readCsvFile("E:\\WorkData\\xinshen\\TRACE.csv")
                .ignoreFirstLine()
                .pojoType(Trace.class,
                        "merchantno", "saledate", "shop", "id", "name", "qty", "amount", "refundqty", "refundamt")
                .setParallelism(10);
        return traceDataSource;
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema.Builder().fields(
                new String[]{"merchantno", "saledate", "shop", "id", "name", "qty", "amount", "refundqty", "refundamt"},
                new DataType[]{DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING(),DataTypes.STRING()}
        ).build();
//        return null;
    }
}
