package test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;

/**
 * @ClassName: MyRetractStream
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/28 14:29
 * @Version 1.0
 **/
public class MyRetractStream implements RetractStreamTableSink {
    @Override
    public TypeInformation getRecordType() {
        return null;
    }

    @Override
    public void emitDataStream(DataStream dataStream) {

    }

    @Override
    public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
        return null;
    }
}
