package com.chengfei.customSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName: MyParalleSource
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/19 16:05
 * @Version 1.0
 * 具有并行度的source
 **/
public class MyParalleSource implements SourceFunction<Long> {

    private long count = 1L;
    private boolean isRunning = true;
    /**
      * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
      * @Date 2019/11/19 16:11
      * @methodName run
      * @Param [sourceContext]
      * @Return void
      **/
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (isRunning){
            sourceContext.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    /**
      * 关闭资源用的方法
      * 取消一个cancel的时候会调用的方法
      * @Date 2019/11/19 16:11
      * @methodName cancel
      * @Param []
      * @Return void
      **/
    @Override
    public void cancel() {
        isRunning = false;
    }

}
