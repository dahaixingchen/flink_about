package com.wsxd.cfs.km.flink.consumeRedisData;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;

/**
 * @ClassName: MyOutput
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/3 19:16
 * @Version 1.0
 **/
public class MyOutput implements OutputFormat<Row> {
    private JedisPool pool;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(100);
        config.setMaxWaitMillis(30000);
        config.setMaxTotal(500);
        config.setMinIdle(50);
        pool = new JedisPool(config, "node-1", 6379, 600000);
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        Jedis resource = pool.getResource();
        resource.lpush("batchFromTable", record.toString());
        resource.close();
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }
}
