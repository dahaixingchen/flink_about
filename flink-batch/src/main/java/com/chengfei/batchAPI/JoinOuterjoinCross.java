package com.chengfei.batchAPI;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * Created by chengfei on 2019/11/20.
 * join, left join , right join , full join , cross(笛卡尔积)
 *
 */
public class JoinOuterjoinCross {

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));


        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(4,"guangzhou"));


        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);


        text1.join(text2).where(0)//指定第一个数据集中需要进行比较的元素角标
                        .equalTo(0)//指定第二个数据集中需要进行比较的元素角标
                        .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                            @Override
                            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second)
                                    throws Exception {
                                return new Tuple3<>(first.f0,first.f1,second.f1);
                            }
                        }).print();

        System.out.println("==================================内连接 join");

        //注意，这里用map和上面使用的with最终效果是一致的。
        /*text1.join(text2).where(0)//指定第一个数据集中需要进行比较的元素角标
                .equalTo(0)//指定第二个数据集中需要进行比较的元素角标
                .map(new MapFunction<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                        return new Tuple3<>(value.f0.f0,value.f0.f1,value.f1.f1);
                    }
                }).print();*/
        /**
         * 左外连接
         *
         * 注意：second这个tuple中的元素可能为null
         *
         */
        text1.leftOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(second==null){
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }else{
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }

                    }
                }).print();

        System.out.println("==================================左外连接 left join");

        /**
         * 右外连接
         *
         * 注意：first这个tuple中的数据可能为null
         *
         */
        text1.rightOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first==null){
                            return new Tuple3<>(second.f0,"null",second.f1);
                        }
                        return new Tuple3<>(first.f0,first.f1,second.f1);
                    }
                }).print();



        System.out.println("==================================右外连接 right join");

        /**
         * 全外连接
         *
         * 注意：first和second这两个tuple都有可能为null
         *
         */

        text1.fullOuterJoin(text2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first==null){
                            return new Tuple3<>(second.f0,"null",second.f1);
                        }else if(second == null){
                            return new Tuple3<>(first.f0,first.f1,"null");
                        }else{
                            return new Tuple3<>(first.f0,first.f1,second.f1);
                        }
                    }
                }).print();
        System.out.println("==================================全外连接 full join");

        text1.cross(text2).print();
        System.out.println("==================================笛卡尔积 cross");
    }



}
