package com.wsxd.cfs.km.flink.consumeCSVData;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @ClassName: MyTableSink
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/3 14:32
 * @Version 1.0
 **/
public class MyTableSink implements BatchTableSink<Row> {

    private TableSchema schema;

    public MyTableSink(TableSchema schema) {
        this.schema = schema;
    }

    @Override
    public DataType getConsumedDataType() {
        return null;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    @Override
    public String[] getFieldNames() {
        return new String[0];
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return new TypeInformation[0];
    }

    @Override
    public void emitDataSet(DataSet<Row> dataSet) {
        dataSet.writeAsFormattedText("C:\\Users\\feifei\\Desktop\\testData\\redis.txt",new MyTextOut());

    }

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        return null;
    }


    private class MyTextOut implements TextOutputFormat.TextFormatter<Row> {
        @Override
        public String format(Row value) {
            return value.toString();
        }
    }
}
