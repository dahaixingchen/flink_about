package com.chengfei.streamAPI;

import com.chengfei.customSource.MyNoParalleSource;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * split
 * 根据规则把一个数据流切分为多个流
 * 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 * @Date 2019/11/20 11:02
 * @Author chengfei
 */
public class SplitSelect {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        //对流进行切分，按照数据的奇偶性进行区分
        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    outPut.add("even");//偶数
                } else {
                    outPut.add("odd");//奇数
                }
                return outPut;
            }
        });
        
        //得到的流只有偶数
        DataStream<Long> evenStream = splitStream.select("even");
        //得到的流只有奇数
        DataStream<Long> oddStream = splitStream.select("odd");

        //选择多个标识，得到的既有奇数又有偶数
        DataStream<Long> moreStream = splitStream.select("odd","even");


        //打印结果
        evenStream.print().setParallelism(1);

        String jobName = SplitSelect.class.getSimpleName();
        env.execute(jobName);
    }
}
