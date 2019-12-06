package jedisTest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;

/**
 * @ClassName: MyredisMapperTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/2 17:25
 * @Version 1.0
 **/
public class MyRedisSourceTest extends RichSourceFunction<List<String>> {

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
    public void run(SourceContext<List<String>> ctx) throws Exception {
        Jedis resource = pool.getResource();
        List<String> strList = resource.lrange("flink-cfsdb.tbl_km_trace", 0, -1);
        ctx.collect(strList);
        resource.close();
    }


    @Override
    public void cancel() {
        pool.close();
    }
}
