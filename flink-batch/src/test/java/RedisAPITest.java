import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName: RedisAPITest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/30 14:05
 * @Version 1.0
 **/
public class RedisAPITest {

    private JedisPoolConfig config;
    private JedisPool pool;
    @BeforeTest
    public void redisConnectionPool(){
        config = new JedisPoolConfig();
        config.setMaxIdle(10);
        config.setMaxWaitMillis(3000);
        config.setMaxTotal(50);
        config.setMinIdle(5);
        pool = new JedisPool(config, "node-1", 6379);
    }

    /**
     *  操作string类型数据
     */
    @Test
    public void strOperate(){
        Jedis resource = pool.getResource();
        //添加
        resource.set("strkey1","strvalue1");
        //查询
        String strvalue1 = resource.get("strkey1");
        System.out.println(strvalue1);
        //修改
        resource.set("strkey1","strNewvalue1");
        String strNewvalue1 = resource.get("strkey1");
        System.out.println(strNewvalue1);
        //删除
//        resource.del("strkey1");
        resource.close();

    }

    /**
     * 操作hash列表类型数据
     */
    @Test
    public void hashOperate(){
        Jedis resource = pool.getResource();
        //添加数据
        resource.hset("hashKey1","hashk1key1","hashk1value1");
        resource.hset("hashKey1","hashk1key2","hashk1value2");
        //获取数据
        Map<String, String> hashKey1 = resource.hgetAll("hashKey1");
        for (String s : hashKey1.values()) {
            System.out.println(s);
        }
        System.out.println("====================================");
        //修改数据
        resource.hset("hashKey1","hashk1key2","hashk1NEWvalue2");
        //获取数据
        Map<String, String> updateHashKey1 = resource.hgetAll("hashKey1");
        for (String s : updateHashKey1.values()) {
            System.out.println(s);
        }
        System.out.println("====================================");
        //获取所有的key
        Set<String> keys = resource.keys("hashKey1");
        for (String key : keys) {
            System.out.println(key);
        }
        //删除数据
        resource.del("hashKey1");
        resource.close();
    }

    /**
     * 操作list类型数据
     */
    @Test
    public void listOperate(){
        Jedis resource = pool.getResource();
        //从左边插入数据(k,(v1,v2,v3,v4.....))
        //数据从左向右插入,最早插入的在右边
        resource.lpush("listkey1","listvalue1","listvalue2","listvalue3","listvalue3","listvalue4");
        //从右面移除数据
        resource.rpop("listkey1");
        //获取所有数据
        List<String> listkey1 = resource.lrange("listkey1", 0, -1);
        for (String s : listkey1) {
            System.out.println(s);
        }
        resource.close();
    }

    /**
     * 操作set类型的数据(与list类型区别是对数据进行去重,数据无序)
     */
    @Test
    public void setOperate(){
        Jedis resource = pool.getResource();
        //添加数据
        resource.sadd("setkey1","setvalue1","setvalue2","setvalue3","setvalue3","setvalue4");
        //查询数据
        Set<String> setkey1 = resource.smembers("setkey1");
        for (String s : setkey1) {
            System.out.println(s);
        }
        //移除掉一个数据
        resource.srem("setkey1","setvalue3");
        resource.close();
    }

    @AfterTest
    public void closePool(){
        pool.close();

    }

}
