package com.wsxd.cfs.km.flink.consumeRedis2Redis.multiThread;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @ClassName: MultiThreadSource
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/5 14:43
 * @Version 1.0
 **/
public class MultiThreadClient implements Runnable {

    private LinkedBlockingQueue<String> bufferQueue;
    private SourceFunction.SourceContext<String> ctx;
    private List<String> newList;

    public MultiThreadClient(LinkedBlockingQueue<String> bufferQueue, SourceFunction.SourceContext<String> ctx) {
        this.ctx = ctx;
        this.bufferQueue = bufferQueue;
    }

    public MultiThreadClient(SourceFunction.SourceContext<String> ctx, List<String> newList) {
        this.ctx = ctx;
        this.newList = newList;
    }

    @Override
    public void run() {
        for (String data:newList){
            ctx.collect(data);
        }
    }
}
