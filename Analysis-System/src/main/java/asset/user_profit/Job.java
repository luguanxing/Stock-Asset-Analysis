package asset.user_profit;

import asset.user_profit.opertators.OperatorBuilder;
import common.pojo.UserAsset;
import common.pojo.UserInout;
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
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job {
    // 获取配置文件
    private static ParameterTool properties = PropertiesUtil.getProperties();

    public static void main(String[] args) {
        // 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration().set(RestOptions.PORT, properties.getInt("flink.webui.user_profit")));
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
        StatementSet statementSet = stEnv.createStatementSet();
        env.setParallelism(1);

        // 读取用户实时资产和出入金数据
        var assetStream = getAssetStream(stEnv);
        var inoutStream = getInoutStream(stEnv);

        // 计算用户盈利
        var profitStream = assetStream
                .connect(inoutStream)
                .keyBy(
                        (KeySelector<UserAsset, Integer>) userAsset -> userAsset.getUid(),
                        (KeySelector<UserInout, Integer>) userInout -> userInout.getUid()
                )
                .flatMap(OperatorBuilder.calculateProfit());

        // 用户资产10秒左右更新，假设和流水相差不超过1秒，可设置盈利计算的会话窗口为5秒左右
        profitStream = profitStream
                .keyBy((KeySelector<UserProfit, Integer>) profit -> profit.getUid())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .reduce((ReduceFunction<UserProfit>) (profit1, profit2) -> {
                    if (profit1.getVersion().compareTo(profit2.getVersion()) < 0) {
                        return profit2;
                    } else {
                        return profit1;
                    }
                });

        // 数据结果保存到mysql和kafka
        stEnv.createTemporaryView("user_profit", profitStream);
        createSinkTable(stEnv);
        statementSet.addInsertSql("insert into kafka_user_profit select uid, profit_value from user_profit");
        statementSet.addInsertSql("insert into mysql_user_profit select uid, profit_value from user_profit");
        statementSet.execute();
    }

    private static SingleOutputStreamOperator<UserAsset> getAssetStream(StreamTableEnvironment stEnv) {
        stEnv.executeSql("" +
                "CREATE TABLE kafka_user_asset (\n" +
                "  uid INT NOT NULL,\n" +
                "  cash_value double,\n" +
                "  position_value double,\n" +
                "  total_value double,\n" +
                "  version AS '0',\n" +
                "  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  kafka_partition BIGINT METADATA FROM 'partition',\n" +
                "  kafka_offset BIGINT METADATA FROM 'offset',\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.asset") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'properties.group.id' = 'user-profit',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        var assetTable = stEnv.sqlQuery("select * from kafka_user_asset");
        return stEnv.toRetractStream(assetTable, UserAsset.class)
                .filter((FilterFunction<Tuple2<Boolean, UserAsset>>) assetTuple2 -> assetTuple2.f0)
                .map((MapFunction<Tuple2<Boolean, UserAsset>, UserAsset>) assetTuple2 -> assetTuple2.f1)
                .keyBy((KeySelector<UserAsset, Integer>) asset -> asset.getUid())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .reduce((ReduceFunction<UserAsset>) (a1, a2) -> {
                    if (a1.getKafka_offset() < a2.getKafka_offset()) {
                        return a2;
                    } else {
                        return a1;
                    }
                });
    }

    private static SingleOutputStreamOperator<UserInout> getInoutStream(StreamTableEnvironment stEnv) {
        stEnv.executeSql("" +
                "CREATE TABLE kafka_user_inout (\n" +
                "  id INT NOT NULL,\n" +
                "  uid INT NOT NULL,\n" +
                "  inout_type STRING,\n" +
                "  inout_value double,\n" +
                "  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  kafka_partition BIGINT METADATA FROM 'partition',\n" +
                "  kafka_offset BIGINT METADATA FROM 'offset',\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.inout") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'properties.group.id' = 'user-profit',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        var assetTable = stEnv.sqlQuery("select * from kafka_user_inout");
        return stEnv.toRetractStream(assetTable, UserInout.class)
                .filter((FilterFunction<Tuple2<Boolean, UserInout>>) inoutTuple2 -> inoutTuple2.f0)
                .map((MapFunction<Tuple2<Boolean, UserInout>, UserInout>) inoutTuple2 -> inoutTuple2.f1)
                .keyBy((KeySelector<UserInout, Integer>) inout -> inout.getId())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .reduce((ReduceFunction<UserInout>) (i1, i2) -> {
                    if (i1.getKafka_offset() < i2.getKafka_offset()) {
                        return i2;
                    } else {
                        return i1;
                    }
                });
    }

    private static void createSinkTable(StreamTableEnvironment stEnv) {
        stEnv.executeSql("" +
                "CREATE TABLE kafka_user_profit (\n" +
                "  uid INT NOT NULL,\n" +
                "  profit_value double,\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.profit") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        stEnv.executeSql("" +
                "CREATE TABLE mysql_user_profit (\n" +
                "  uid INT NOT NULL,\n" +
                "  profit_value double,\n" +
                "  PRIMARY KEY (`uid`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                String.format("   'url' = 'jdbc:mysql://%s/%s',\n", properties.get("output.db.host"), properties.get("output.db.database")) +
                "   'table-name' = '" + properties.get("output.tb.profit") + "',\n" +
                "   'username' = '" + properties.get("output.db.username") + "',\n" +
                "   'password' = '" + properties.get("output.db.password") + "'\n" +
                ")" +
                "");
    }
}
