package com.wsxd.cfs.km.flink.redis2redis;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

/**
 * @ClassName: MyRedisSource
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/30 16:53
 * @Version 1.0
 **/
public class MyRedisSource extends RichSourceFunction<String> {

    private JedisPoolConfig config;
    private JedisPool pool;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        config = new JedisPoolConfig();
        config.setMaxIdle(100);
        config.setMaxWaitMillis(30000);
        config.setMaxTotal(500);
        config.setMinIdle(50);
        pool = new JedisPool(config,"node-1",6379,600000);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Jedis resource = pool.getResource();
        List<String> strList = resource.lrange("flink-cfsdb.tbl_km_trace", 0, -1);
        for (int i = 0;i < strList.size();i++){
            ctx.collect(strList.get(i));
        }

        resource.close();
    }

    @Override
    public void cancel() {
        pool.close();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
