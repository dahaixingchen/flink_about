package test.redis;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

/**
 * @ClassName: RedisTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/2 9:31
 * @Version 1.0
 **/
public class RedisTest {
    private JedisPool pool;
    @BeforeTest
    public void connectRedisPool(){
        JedisPoolConfig config = new JedisPoolConfig();
        config = new JedisPoolConfig();
        config.setMaxIdle(10);
        config.setTimeBetweenEvictionRunsMillis(60000);
        config.setSoftMinEvictableIdleTimeMillis(60000);
        config.setMaxWaitMillis(300000);
        config.setMaxTotal(50);
        config.setMinIdle(5);
        pool = new JedisPool(config,"node-1",6379,60000);
    }

    @Test
    public void redisTest(){
        Jedis resource = pool.getResource();

        List<String> dataList = resource.lrange("flink-cfsdb.tbl_km_trace-largeData", 0, -1);
        int sum = 0;
        for (String s:dataList){
            sum =+1;
//            System.out.println(s);
        }
        System.out.println(dataList.size());
        resource.close();
    }
}
