package com.chengfei.dynamicTable;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

/**
 * @ClassName: MyUpsertSink
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/28 14:03
 * @Version 1.0
 **/
//UpsertStreamTableSink<Tuple2<String, Long>> {
public class MyUpsertSink implements UpsertStreamTableSink<Tuple2<String,Long>>{
    private TableSchema schema; //表结构
    private String[] keyFields;
    private boolean isAppendOnly;

    private String[] fieldName;
    private TypeInformation<?>[] fieldTypes;

    //这个方法可以不要但是作为默认构造器，最好还是保留
    public MyUpsertSink() {
    }

    public MyUpsertSink(TableSchema schema){
        this.schema = schema;
    }

    @Override
    public void setKeyFields(String[] keys) {
        this.keyFields = keys;
    }

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {
        this.isAppendOnly = isAppendOnly;
    }

    @Override
    public TypeInformation<Tuple2<String, Long>> getRecordType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {});
    }

    //这个方法也不是必须的，可以不用实现它
    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
//        consumeDataStream(dataStream);
    }

    //这个是输出結果的方法
    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
        return dataStream.addSink(new MyDataSink()).setParallelism(1);
    }

    //这个方法也不是必须的，可以不用实现它
    @Override
    public TableSink<Tuple2<Boolean, Tuple2<String, Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
//        MyUpsertSink myUpsertSink = new MyUpsertSink();
//        myUpsertSink.setIsAppendOnly(isAppendOnly);
//        myUpsertSink.setFieldName(fieldName);
//        myUpsertSink.setFieldTypes(fieldTypes);
//        myUpsertSink.setKeyFields(keyFields);
//        return myUpsertSink;
        return null;
    }

    //得到传进来的表的
    @Override
    public String[] getFieldNames() {
        return schema.getFieldNames();
    }



    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return schema.getFieldTypes();
    }

    //这个类就是streamSink的类，负责把数据弄到哪里去
    private class MyDataSink extends RichSinkFunction<Tuple2<Boolean, Tuple2<String, Long>>> {
        public MyDataSink(){}
        @Override
        public void invoke(Tuple2<Boolean, Tuple2<String, Long>> value, Context context) throws Exception {
            System.out.println("发送消息：" + value);
        }
    }

}
