package com.wsxd.cfs.km.flink.consumeRedis2Redis.multiThread;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.concurrent.*;

/**
 * @ClassName: MyredisMapperTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/2 17:25
 * @Version 1.0
 **/
public class MyRedisSourceMultiThread extends RichSourceFunction<String> {

    private  JedisPoolConfig config;
    private JedisPool pool;
    private List<String> strList;

    // Client 线程的默认数量
    private final int DEFAULT_CLIENT_THREAD_NUM = 10;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        config = new JedisPoolConfig();
        config.setMaxIdle(100);
        config.setMaxWaitMillis(30000);
        config.setMaxTotal(500);
        config.setMinIdle(50);
        pool = new JedisPool(config,"node-1",6379,600000);
        Jedis resource = pool.getResource();
//        strList = resource.lrange("flink-cfsdb.tbl_km_trace", 0, -1);
        strList = resource.lrange("flink-cfsdb.tbl_km_trace", 0, -1);
        resource.close();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {

        //每个线程需要处理的数据
        int needProcessedNum = strList.size()/DEFAULT_CLIENT_THREAD_NUM+1;
        List<String> newList = null;

        CountDownLatch countDownLatch = new CountDownLatch(3);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(DEFAULT_CLIENT_THREAD_NUM);

        //开启多线程去消费
        for (int i = 0;i < DEFAULT_CLIENT_THREAD_NUM; i++){
            if ((i+1) == DEFAULT_CLIENT_THREAD_NUM){
                int startIndex = i * needProcessedNum;
                newList =  strList.subList(startIndex,strList.size());
            }else {
                int startIndex = i * needProcessedNum;
                int endIndex = (i+1) * needProcessedNum;
                newList = strList.subList(startIndex,endIndex);
            }
            MultiThreadClient multiThreadClient = new MultiThreadClient(ctx,newList);
//            threadPoolExecutor.execute(multiThreadClient);
            Thread thread = new Thread(multiThreadClient);
            thread.start();
            thread.join();
        }

    }

    @Override
    public void cancel() {
        pool.close();

    }
}
