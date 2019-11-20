package com.chengfei.batchAPI;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * 获取集合中的前N个元素
 * Created by chengfei on 2019/11/20.
 */
public class GroupbySortFirstN {

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"zs"));
        data.add(new Tuple2<>(4,"ls"));
        data.add(new Tuple2<>(3,"ww"));
        data.add(new Tuple2<>(1,"xw"));
        data.add(new Tuple2<>(1,"aw"));
        data.add(new Tuple2<>(1,"bw"));
        data.add(new Tuple2<>(1,"ab"));
        data.add(new Tuple2<>(1,"aa"));


        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);


        //获取前3条数据，按照数据插入的顺序
        text.first(3).print();
        System.out.println("===获取前3条数据，按照数据插入的顺序");

        //根据数据中的第一列进行分组，获取每组的前2个元素
        text.groupBy(0).first(2).print();
        System.out.println("===根据数据中的第一列进行分组，获取每组的前2个元素");

        //根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("===根据数据中的第一列分组，再根据第二列进行组内排序[升序]，获取每组的前2个元素");

        //不分组，全局排序获取集合中的前3个元素，针对第一个元素升序，第二个元素倒序
        text.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3).print();
        System.out.println("===不分组，全局排序获取集合中的前3个元素，针对第一个元素升序，第二个元素倒序");

    }



}
