package com.chengfei.sql.operations;

import com.chengfei.dynamicTable.MyUpsertSink;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;

/**
 * @ClassName: Select
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/28 16:36
 * @Version 1.0
 **/
public class Select {
    public static void main(String[] args) throws Exception {
//        TableEnvironment tabEnv = TableEnvironment.create(
//                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());


//        Table soTable = tabEnv.fromTableSource(new MyTableSource2());
//        tabEnv.registerTable("soTable",soTable);

        DataStreamSource<Tuple2<String, String>> soData = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Liz", "./prod?id=3"),
                new Tuple2<>("Mary", "./prod?id=7")
        );
        Table soTab = tabEnv.fromDataStream(soData,"user,url");
        tabEnv.registerTable("clicks",soTab);

        Table selectTab = tabEnv.sqlQuery("select user,count(url) from clicks group by user order by user");

        tabEnv.registerTableSink("result",new MyUpsertSink(selectTab.getSchema()));
//        tabEnv.registerTableSink("result",new MyAppendTableSink(selectTab.getSchema()));
        selectTab.insertInto("result");

        tabEnv.execute("selfSink and source");
    }

    private static class MyTableSource2 implements StreamTableSource{

        @Override
        public DataStream getDataStream(StreamExecutionEnvironment execEnv) {
            return execEnv.fromElements(
                    new Tuple2<>("Mary", "./home"),
                    new Tuple2<>("Bob", "./cart"),
                    new Tuple2<>("Mary", "./prod?id=1"),
                    new Tuple2<>("Liz", "./home"),
                    new Tuple2<>("Liz", "./prod?id=3"),
                    new Tuple2<>("Mary", "./prod?id=7")
            );
        }

        @Override
        public TableSchema getTableSchema() {
            return new TableSchema(new String[]{"user", "url"}, new TypeInformation[]{Types.STRING, Types.STRING});
        }
    }

    //数据源
    private static class MyTableSource implements StreamTableSource{
        @Override
        public DataStream getDataStream(StreamExecutionEnvironment execEnv) {
            DataStreamSource<Tuple3<String, String, String>> soData = execEnv.fromElements(
                    new Tuple3<>("Mary", "./home", "2017-11-26T09:15:05Z"),
                    new Tuple3<>("Bob", "./cart", "2017-11-26T09:15:05Z"),
                    new Tuple3<>("Mary", "./prod?id=1", "2017-11-26T09:15:05Z"),
                    new Tuple3<>("Liz", "./home", "2017-11-26T09:15:05Z"),
                    new Tuple3<>("Bob", "./prod?id=3", "2017-11-26T09:15:05Z")
            );
            return soData;
        }

        @Override
        public TableSchema getTableSchema() {
            TableSchema tableSchema = new TableSchema(new String[]{"user", "url", "time"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.SQL_TIMESTAMP});
            return tableSchema;
        }
    }

    private static class MyAppendTableSink implements AppendStreamTableSink {
        private TableSchema schema;

        private String[] fieldNames;
        private TypeInformation[] fieldTypes;

        public void setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        public void setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
        }


        @Override
        public TypeInformation getOutputType() {
            return TypeInformation.of(new TypeHint<Tuple3<String,String,String>>() {});
        }

        @Override
        public String[] getFieldNames() {
            return schema.getFieldNames();
        }

        @Override
        public TypeInformation[] getFieldTypes() {
            return schema.getFieldTypes();
        }

        public MyAppendTableSink() {
        }

        public MyAppendTableSink(TableSchema schema) {
            this.schema = schema;
        }

        @Override
        public void emitDataStream(DataStream dataStream) {
            dataStream.addSink(new MySink());
        }

        @Override
        public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
            MyAppendTableSink myAppendTableSink = new MyAppendTableSink();
            myAppendTableSink.setFieldNames(fieldNames);
            myAppendTableSink.setFieldTypes(fieldTypes);;
            return myAppendTableSink ;
        }

        private class MySink implements SinkFunction {
            @Override
            public void invoke(Object value) throws Exception {
                System.out.println("发送的数据：" + value);
            }
        }
    }

}
