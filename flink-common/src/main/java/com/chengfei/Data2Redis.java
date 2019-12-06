package com.chengfei;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.*;
import java.util.stream.Stream;

/**
 * @ClassName: Data2Redis
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/30 14:55
 * @Version 1.0
 * 把数据灌到Redis中
 **/
public class Data2Redis {
    private static JedisPoolConfig config ;
    private static JedisPool pool;
    public static void connectJedis(){
        config = new JedisPoolConfig();
        config.setMaxIdle(10);
        config.setMaxWaitMillis(30000);
        config.setMaxTotal(50);
        config.setMinIdle(5);
        pool = new JedisPool(config, "node-1", 6379);
    }
    public static void main(String[] args) throws Exception {
        //连接Redis
        connectJedis();
        Jedis resource = pool.getResource();
        File file = new File("C:\\Users\\feifei\\Desktop\\testData\\flinkTestLess - 副本.txt");
//        File file = new File("/root/flinkTestLess.txt");
        System.out.println(file.length());
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        String tem ;
//        if (resource.exists("flink-cfsdb.tbl_km_trace-largeData")){
//            resource.del("flink-cfsdb.tbl_km_trace-largeData");
//        }

        while ((tem = fileReader.readLine()) != null){
            resource.lpush("flink-cfsdb.tbl_km_trace-test",tem);
        }
//        System.out.println(resource.lrange("flink-cfsdb.tbl_km_trace",0,-1));
        System.out.println(resource.llen("flink-cfsdb.tbl_km_trace-largeData"));
    }

}
