package com.wsxd.cfs.km.flink;

import com.wsxd.cfs.km.flink.pojo.TblKmTrace;
import com.wsxd.cfs.km.flink.source.MySourceFromMysql;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: TblKmTrace
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/21 11:27
 * @Version 1.0
 **/
public class TblKmTraceFlink {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<TblKmTrace> sourceData = env.addSource(new MySourceFromMysql());
        SingleOutputStreamOperator<TblKmTrace> mapData = sourceData.map(new MapFunction<TblKmTrace, TblKmTrace>() {
            @Override
            public TblKmTrace map(TblKmTrace tblKmTrace) throws Exception {
                return tblKmTrace;
            }
        });
        mapData.print();

        try {
            env.execute("TblKmTraceFlink");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
