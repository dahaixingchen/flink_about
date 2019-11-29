package com.chengfei.streamSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @ClassName: Mysource
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/21 11:35
 * @Version 1.0
 **/
public class MySourceFromMysql extends RichSourceFunction {
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
