package com.chengfei.batchAPI;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Created by chengfei on 2019/11/20.
 * 对这批数据进行去重
 * 对应的也有对那个字段去重
 */
public class Distinct {

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);

        DataSet<String> flatMapData = text.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.toLowerCase().split("\\W+");
                for (String word : split) {
                    System.out.println("单词："+word);
                    out.collect(word);
                }
            }
        });

        flatMapData.distinct()// 对数据进行整体去重
                .writeAsText("E:\\WorkData\\testData\\rides.txt");


    }



}
