package com.chengfei.streamAPI;

import com.chengfei.customSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
  *
  * @Date 2019/11/20 11:02
  * @Author chengfei
  * partition在flink中的物理变现是线程，多少分区对应的多少的线程
  * flink还提供了随机分区，用法：(dataStream)tupleData.shffle()就可以实现
  * 还有比较好用的rebalance()，它可以很好的解决负载均衡，实现全量的平均分配数据量，用法类似上面
  * rescale也是负载均衡的分区方法，他跟rebalance的区别就是它会根据所在服务器中的分区进行负载均衡，不会全量，用法类似
  * broadcast也是一种数据分发方式，它的特点会把上一步的每条数据广播（分发）给下游的每个分区
  **/
public class MyPartition implements Partitioner<Long> {
    @Override
    public int partition(Long key, int numPartitions) {
        System.out.println("分区总数："+numPartitions);
        if(key % 2 == 0){
            return 0;
        }else{
            return 1;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        //对数据进行转换，把long类型转成tuple1类型
        DataStream<Tuple1<Long>> tupleData = text.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1<Long> map(Long value) throws Exception {
                System.out.println("线程的 filterid ：" + Thread.currentThread().getId() );
                return new Tuple1<>(value);
            }
        });
        //分区之后的数据
        DataStream<Tuple1<Long>> partitionData= tupleData.partitionCustom(new MyPartition(), 0);

        DataStream<Long> result = partitionData.map(new MapFunction<Tuple1<Long>, Long>() {
            @Override
            public Long map(Tuple1<Long> value) throws Exception {
                System.out.println("当前线程 id ：" + Thread.currentThread().getId() + ",value: " + value);
                return value.getField(0);
            }
        });
        result.print().setParallelism(1);

        env.execute("SteamingDemoWithMyParitition");

    }

}
