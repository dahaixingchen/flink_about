package com.wsxd.cfs.km.flink.realTimeDataWarehouse.fullputler;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName: HbaseOutputFormat
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/12 15:16
 * @Version 1.0
 **/
public class HbaseOutputFormat implements OutputFormat<Row> {
    private String zkAddress;
    private String tableName;
    private String family;
    private Connection conn;
    private Configuration cof;

    public HbaseOutputFormat(String zkAddress, String tableName, String family) {
        this.zkAddress = zkAddress;
        this.tableName = tableName;
        this.family = family;
    }

    @Override
    public void configure(org.apache.flink.configuration.Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        cof = HBaseConfiguration.create();
        cof.set("hbase.zookeeper.quorum",zkAddress);
        conn = ConnectionFactory.createConnection(cof);
    }

    @Override
    public void writeRecord(Row record) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes( record.getField(0).toString()));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes("goodsId"),Bytes.toBytes(record.getField(0).toString()));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes("goodsName"),Bytes.toBytes(record.getField(1).toString()));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes("sellingPrice"),Bytes.toBytes(record.getField(2).toString()));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes("goodsStock"),Bytes.toBytes(record.getField(3).toString()));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes("appraiseNum"),Bytes.toBytes(record.getField(4).toString()));
        table.put(put);
        table.close();
    }

    @Override
    public void close() throws IOException {
        conn.close();
    }
}
