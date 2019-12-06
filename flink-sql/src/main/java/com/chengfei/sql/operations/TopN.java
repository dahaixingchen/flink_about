package com.chengfei.sql.operations;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

/**
 * @ClassName: TopN
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/4 10:04
 * @Version 1.0
 **/
public class TopN {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        DataStream<Tuple4<String, String, String, Long>> ds = env.fromElements(
                new Tuple4<>("122", "fei", "52",55L),
                new Tuple4<>("125", "xu", "26",22L),
                new Tuple4<>("422", "xuxu","",96L),
                new Tuple4<>("122", "xuxu", "",85L),
                new Tuple4<>("422", "xuxu", "",52L),
                new Tuple4<>("125", "zhang", "",45L)
        );
        tableEnv.registerDataStream("ShopSales", ds, "product_id, category, product_name, sales");
//        env.setStateBackend(new RocksDBStateBackend("hdfs://node-1:8020/flink/checkpoints"),true);
//        env.setStateBackend(new FsStateBackend("hdfs://node-1:8020/flink/checkpoints"));
        env.setStateBackend(new StateBackend() {
            @Override
            public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
                return null;
            }

            @Override
            public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
                return null;
            }

            @Override
            public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(Environment env, JobID jobID, String operatorIdentifier, TypeSerializer<K> keySerializer, int numberOfKeyGroups, KeyGroupRange keyGroupRange, TaskKvStateRegistry kvStateRegistry, TtlTimeProvider ttlTimeProvider, MetricGroup metricGroup, @Nonnull Collection<KeyedStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
                return null;
            }

            @Override
            public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @Nonnull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
                return null;
            }
        });
        Table result1 = tableEnv.sqlQuery(
                "SELECT * " +
                        "FROM (" +
                        "   SELECT *," +
                        "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
                        "   FROM ShopSales)" +
                        "WHERE row_num <= 2");
        tableEnv.toRetractStream(result1, TypeInformation.of(new TypeHint<Row>() {
        })).print();
        env.execute("sql_topN");

        //flink的批处理不支持ROW_NUMBER（）的语法（blink的batch应该支持）

//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
//        DataSource<Tuple4<String, String, String, Long>> ds = env.fromElements(
//                new Tuple4<>("122", "fei", "52", 55L),
//                new Tuple4<>("125", "xu", "26", 22L),
//                new Tuple4<>("422", "xuxu", "", 96L),
//                new Tuple4<>("122", "xuxu", "", 85L),
//                new Tuple4<>("422", "xuxu", "", 52L),
//                new Tuple4<>("125", "zhang", "", 45L)
//        );
//        tableEnv.registerDataSet("ShopSales", ds, "product_id, category, product_name, sales");
//        Table result1 = tableEnv.sqlQuery(
//                "SELECT * " +
//                        "FROM (" +
//                        "   SELECT *," +
//                        "       ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as row_num" +
//                        "   FROM ShopSales)" +
//                        "WHERE row_num <= 2");
//        tableEnv.toDataSet(result1,TypeInformation.of(new TypeHint<Row>() {
//        })).print();
    }
}
