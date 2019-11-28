package test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * @ClassName: MyAppendStream
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/28 14:30
 * @Version 1.0
 **/
public class MyAppendStream implements AppendStreamTableSink {
    @Override
    public void emitDataStream(DataStream dataStream) {

    }

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        return null;
    }
}
