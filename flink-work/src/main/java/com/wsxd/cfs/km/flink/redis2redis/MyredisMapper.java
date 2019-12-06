package com.wsxd.cfs.km.flink.redis2redis;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.types.Row;

/**
 * @ClassName: MyredisSink
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/2 10:29
 * @Version 1.0
 **/
public class MyredisMapper implements RedisMapper<Row> {

    @Override
    public RedisCommandDescription getCommandDescription() {

        return new RedisCommandDescription(RedisCommand.LPUSH);
    }

    @Override
    public String getKeyFromData(Row data) {
        return "flink-cfsdb.tbl_km_trace-result-large";

    }

    @Override
    public String getValueFromData(Row data) {
        return data.toString();
    }
}
