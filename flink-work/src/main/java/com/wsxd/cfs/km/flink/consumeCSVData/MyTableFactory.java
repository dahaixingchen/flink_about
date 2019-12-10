package com.wsxd.cfs.km.flink.consumeCSVData;

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.BatchTableSinkFactory;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.table.sinks.TableSink;

import java.util.List;
import java.util.Map;

/**
 * @ClassName: MyTableFactory
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/3 18:17
 * @Version 1.0
 **/
public class MyTableFactory implements BatchTableSinkFactory {
    @Override
    public BatchTableSink createBatchTableSink(Map properties) {
        return null;
    }

    @Override
    public Map<String, String> requiredContext() {
        return null;
    }

    @Override
    public List<String> supportedProperties() {
        return null;
    }

    @Override
    public TableSink createTableSink(Map properties) {
//        new batch
        return null;
    }

    @Override
    public TableSink createTableSink(ObjectPath tablePath, CatalogTable table) {
        return null;
    }
}
