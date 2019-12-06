package com.chengfei.batchHight;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.testng.annotations.Test;

/**
 * 全局累加器
 * counter 计数器
 *
 * 需求：
 * 计算map函数中处理了多少数据
 *
 * 注意：只有在任务执行结束后，才能获取到累加器的值
 *
 * Created by chengfei on 2019/11/20.
 */
public class Counter {

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            //1:创建累加器
           private IntCounter numLines = new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2:注册累加器
                getRuntimeContext().addAccumulator("num-lines",this.numLines);

            }
            //int sum = 0;
            @Override
            public String map(String value) throws Exception {
                //如果并行度为1，使用普通的累加求和即可，但是设置多个并行度，则普通的累加求和结果就不准了
                //sum++;
                //System.out.println("sum："+sum);
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(8);

        //result.print();

        result.writeAsText("d:\\data\\count10\\rides.txt");

        JobExecutionResult jobResult = env.execute("counter");
        //3：获取累加器
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num:"+num);

    }

    @Test
    public void testCounter() throws Exception {
        //不能通过全局变量的方式来累加，因为map是内部类
//        int sum = 0;
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.fromElements("a", "b", "c", "d","a", "b", "c", "d");
        text.map(new MapFunction<String, String>() {

            int sum = 0;
            @Override
            public String map(String s) throws Exception {
                sum += 1;
                System.out.println("sum:" + sum);
                return s;
            }
        }).setParallelism(3).print();
    }



}
