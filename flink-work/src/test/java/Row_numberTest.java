import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: Row_numberTest
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/12/4 9:42
 * @Version 1.0
 **/
public class Row_numberTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        DataStream<Tuple3<Long, String, Integer>> ds = env.fromElements(
                new Tuple3<>(122L, "fei", 52),
                new Tuple3<>(121L, "xu", 63),
                new Tuple3<>(126L, "feifei",6),
                new Tuple3<>(123L, "xuxu", 52),
                new Tuple3<>(122L, "cheng", 10),
                new Tuple3<>(120L, "zhang", 41)
        );
        tableEnv.registerDataStream("Orders",ds,"user, product, amount, proctime.proctime, rowtime.rowtime");

        Table result1 = tableEnv.sqlQuery(
                "SELECT user, " +
                        "  TUMBLE_START(rowtime, INTERVAL '1' DAY) as wStart,  " +
                        "  SUM(amount) FROM Orders " +
                        "GROUP BY TUMBLE(rowtime, INTERVAL '1' DAY), user");
        tableEnv.toRetractStream(result1, TypeInformation.of(new TypeHint<Row>() {
        })).print();

        env.execute("sql");
    }
}
