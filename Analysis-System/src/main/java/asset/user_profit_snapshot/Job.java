package asset.user_profit_snapshot;

import asset.user_profit_snapshot.operators.OperatorBuilder;
import common.pojo.UserProfit;
import common.utils.PropertiesUtil;
import lombok.var;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job {
    // 获取配置文件
    private static ParameterTool properties = PropertiesUtil.getProperties();

    public static void main(String[] args) {
        // 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration().set(RestOptions.PORT, properties.getInt("flink.webui.user_profit_snapshot")));
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        // 读取实时数仓消息队列中用户盈利，定时进行快照输出
        var profitStream = getProfitStream(stEnv);
        var snapshotStream = profitStream
                .keyBy((KeySelector<UserProfit, Integer>) profit -> profit.getUid())
                .process(OperatorBuilder.doSnapshot());

        // 数据结果保存到mysql
        stEnv.createTemporaryView("user_profit_snapshot", snapshotStream);
        createSinkTable(stEnv);
        stEnv.executeSql("insert into mysql_user_profit_snapshot select uid, update_time, profit_value from user_profit_snapshot");
    }

    private static SingleOutputStreamOperator<UserProfit> getProfitStream(StreamTableEnvironment stEnv) {
        stEnv.executeSql("" +
                "CREATE TABLE kafka_user_profit (\n" +
                "  uid INT NOT NULL,\n" +
                "  profit_value double,\n" +
                "  version AS '0',\n" +
                "  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  kafka_partition BIGINT METADATA FROM 'partition',\n" +
                "  kafka_offset BIGINT METADATA FROM 'offset',\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.profit") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'properties.group.id' = 'user-asset-snapshot',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        var profitTable = stEnv.sqlQuery("select * from kafka_user_profit");
        return stEnv.toRetractStream(profitTable, UserProfit.class)
                .filter((FilterFunction<Tuple2<Boolean, UserProfit>>) profitTuple2 -> profitTuple2.f0)
                .map((MapFunction<Tuple2<Boolean, UserProfit>, UserProfit>) profitTuple2 -> profitTuple2.f1)
                .keyBy((KeySelector<UserProfit, Integer>) asset -> asset.getUid())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .reduce((ReduceFunction<UserProfit>) (p1, p2) -> {
                    if (p1.getKafka_offset() < p2.getKafka_offset()) {
                        return p2;
                    } else {
                        return p1;
                    }
                });
    }

    private static void createSinkTable(StreamTableEnvironment stEnv) {
        stEnv.executeSql("" +
                "CREATE TABLE mysql_user_profit_snapshot (\n" +
                "  uid INT NOT NULL,\n" +
                "  ts BIGINT,\n" +
                "  profit double,\n" +
                "  PRIMARY KEY (`uid`, `ts`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                String.format("   'url' = 'jdbc:mysql://%s/%s',\n", properties.get("output.db.host"), properties.get("output.db.database")) +
                "   'table-name' = '" + properties.get("output.tb.profit_snapshot") + "',\n" +
                "   'username' = '" + properties.get("output.db.username") + "',\n" +
                "   'password' = '" + properties.get("output.db.password") + "'\n" +
                ")" +
                "");
    }

}
