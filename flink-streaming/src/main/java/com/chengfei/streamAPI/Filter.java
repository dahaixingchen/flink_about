package com.chengfei.streamAPI;

import com.chengfei.customSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 *
 * @Date 2019/11/20 11:10
 * @Author chengfei
 * filter
 * 过滤掉一些流
 **/
public class Filter {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        DataStream<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override

            public Long map(Long value) throws Exception {
                System.out.println("*****************************************");
                System.out.println("线程的 mapid ：" + Thread.currentThread().getId() );
                System.out.println("*****************************************");
                System.out.println("原始接收到数据：" + value);
                return value;
            }
        });

        //默认就是按照8个并行度跑数据
        //执行filter过滤，满足条件的数据会被留下
        DataStream<Long> filterData = num.filter(new FilterFunction<Long>() {
            //把所有的奇数过滤掉,只留下运算后为true的数据，为false的数据都过滤掉
            @Override
            public boolean filter(Long value) throws Exception {
                System.out.println("*****************************************");
                System.out.println("线程的 filterid ：" + Thread.currentThread().getId() );
                System.out.println("*****************************************");
                return value % 2 == 0;
            }
        });

        //这个方法在8可以跑8线程的CPU的情况下最多只能4个并行度
        DataStream<Long> resultData = filterData.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("*****************************************");
                System.out.println("线程的 filtMapid ：" + Thread.currentThread().getId() );
                System.out.println("*****************************************");
                System.out.println("：过滤之后的数据：" + value);
                return value;
            }
        }).setParallelism(2);


        //每2秒钟处理一次数据
        DataStream<Long> sum = resultData.timeWindowAll(Time.seconds(2)).sum(0);

        //打印结果
        sum.print().setParallelism(1);

        String jobName = Filter.class.getSimpleName();
        env.execute(jobName);
    }
}
