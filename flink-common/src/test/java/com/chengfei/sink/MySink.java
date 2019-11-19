package com.chengfei.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ClassName: MySink
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/19 16:07
 * @Version 1.0
 **/
public class MySink extends RichParallelSourceFunction {
    @Override
    public void run(SourceContext sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}
