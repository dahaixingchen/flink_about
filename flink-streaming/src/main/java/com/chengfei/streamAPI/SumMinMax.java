package com.chengfei.streamAPI;

import com.chengfei.customSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * sum,min,max演示
 *
 * @Date 2019/11/20 11:02
 * @Author chengfei
 */
public class SumMinMax {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());//注意：针对此source，并行度只能设置为1
        DataStream<Tuple2<Long, Long>> mapData = text.rebalance().map(new MapFunction<Long, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(Long value) throws Exception {
                return new Tuple2<Long, Long>(value, 1L);
            }
        });

        //没有选用窗口函数看不出结果
//        mapData.keyBy(0).max(0).print();
//        mapData.keyBy(0).sum(0).print();
        mapData.keyBy(0).min(0).print();

        env.execute("StreamingDemoSumMinMax");
    }
}
