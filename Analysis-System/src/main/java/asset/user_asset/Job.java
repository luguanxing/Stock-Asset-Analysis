package asset.user_asset;

import asset.user_asset.operators.OperatorBuilder;
import common.pojo.*;
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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job {
    // 获取配置文件
    private static ParameterTool properties = PropertiesUtil.getProperties();

    public static void main(String[] args) throws Exception {
        // 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration().set(RestOptions.PORT, properties.getInt("flink.webui.user_asset")));
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment stEnv = StreamTableEnvironment.create(env, settings);
        StatementSet statementSet = stEnv.createStatementSet();
        env.setParallelism(1);

        // 读取实时数仓消息队列中钱和货数据，做session窗口保证数据稳定性
        var cashStream = stEnv
                .toRetractStream(getCashTable(stEnv), UserCash.class)
                .keyBy((KeySelector<Tuple2<Boolean, UserCash>, Integer>) cashTuple2 -> cashTuple2.f1.getUid())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
                .reduce((ReduceFunction<Tuple2<Boolean, UserCash>>) (c1, c2) -> {
                    Boolean dml1 = c1.f0;
                    Boolean dml2 = c2.f0;
                    UserCash cash1 = c1.f1;
                    UserCash cash2 = c2.f1;
                    if (cash1.getKafka_offset() < cash2.getKafka_offset()) {
                        return c2;
                    } else if (cash1.getKafka_offset() > cash2.getKafka_offset()) {
                        return c1;
                    } else {
                        if (!dml1) {
                            return c1;
                        }
                        if (!dml2) {
                            return c2;
                        }
                        return c2;
                    }
                });
        var positionStream = stEnv
                .toRetractStream(getPositionTable(stEnv), UserPosition.class)
                .keyBy((KeySelector<Tuple2<Boolean, UserPosition>, Integer>) cashTuple2 -> cashTuple2.f1.getUid())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)))
                .reduce((ReduceFunction<Tuple2<Boolean, UserPosition>>) (p1, p2) -> {
                    Boolean dml1 = p1.f0;
                    Boolean dml2 = p2.f0;
                    UserPosition position1 = p1.f1;
                    UserPosition postion2 = p2.f1;
                    if (position1.getKafka_offset() < postion2.getKafka_offset()) {
                        return p2;
                    } else if (position1.getKafka_offset() > postion2.getKafka_offset()) {
                        return p1;
                    } else {
                        if (!dml1) {
                            return p1;
                        }
                        if (!dml2) {
                            return p2;
                        }
                        return p2;
                    }
                });
        var quotationStream = stEnv.toRetractStream(getQuotationTable(stEnv), StockQuotation.class)
                .filter((FilterFunction<Tuple2<Boolean, StockQuotation>>) quotationTuple2 -> quotationTuple2.f0)
                .map((MapFunction<Tuple2<Boolean, StockQuotation>, StockQuotation>) quotationTuple2 -> quotationTuple2.f1)
                .keyBy((KeySelector<StockQuotation, String>) stockQuotation -> stockQuotation.getStock_id())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .reduce((ReduceFunction<StockQuotation>) (q1, q2) -> {
                    if (q1.getKafka_offset() < q2.getKafka_offset()) {
                        return q2;
                    } else {
                        return q1;
                    }
                });

        // 持仓关联报价
        var positionEtlStream = positionStream
                .connect(quotationStream)
                .keyBy(
                        (KeySelector<Tuple2<Boolean, UserPosition>, String>) positionTuple -> positionTuple.f1.getStock_id(),
                        (KeySelector<StockQuotation, String>) quotation -> quotation.getStock_id()
                )
                .flatMap(OperatorBuilder.enrichPositionWithQuotation());

        // 计算用户资产
        var userAssetStream = cashStream
                .connect(positionEtlStream)
                .keyBy(
                        (KeySelector<Tuple2<Boolean, UserCash>, Integer>) cashTuple2 -> cashTuple2.f1.getUid(),
                        (KeySelector<Tuple2<Boolean, UserPositionEtl>, Integer>) positionTuple2 -> positionTuple2.f1.getUid()
                )
                .flatMap(OperatorBuilder.calculateUserAsset());

        // 用户资产做session窗口，假设cash/position事务更新间隔不超过5秒，报价限制窗口30秒更新一次，那么10秒的session能保证平稳性
        userAssetStream = userAssetStream
                .keyBy((KeySelector<UserAsset, Integer>) userAsset -> userAsset.getUid())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .reduce((ReduceFunction<UserAsset>) (asset1, asset2) -> {
                    if (asset1.getVersion().compareTo(asset2.getVersion()) < 0) {
                        return asset2;
                    } else {
                        return asset1;
                    }
                });

        // 数据结果保存到mysql和kafka
        stEnv.createTemporaryView("user_asset", userAssetStream);
        createSinkTable(stEnv);
        statementSet.addInsertSql("insert into kafka_user_asset select uid, cash_value, position_value, total_value from user_asset");
        statementSet.addInsertSql("insert into mysql_user_asset select uid, cash_value, position_value, total_value from user_asset");
        statementSet.execute();
    }

    private static Table getCashTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql("" +
                "CREATE TABLE kafka_user_cash (\n" +
                "  uid INT NOT NULL,\n" +
                "  cash_value double,\n" +
                "  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  kafka_partition BIGINT METADATA FROM 'partition',\n" +
                "  kafka_offset BIGINT METADATA FROM 'offset',\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.cash") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'properties.group.id' = 'user-asset',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        return tEnv.sqlQuery("select * from kafka_user_cash");
    }

    private static Table getPositionTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql("" +
                "CREATE TABLE kafka_user_position (\n" +
                "  uid INT NOT NULL,\n" +
                "  stock_id STRING NOT NULL,\n" +
                "  quantity double,\n" +
                "  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  kafka_partition BIGINT METADATA FROM 'partition',\n" +
                "  kafka_offset BIGINT METADATA FROM 'offset',\n" +
                "  PRIMARY KEY (uid, stock_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.position") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'properties.group.id' = 'user-asset',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        return tEnv.sqlQuery("select * from kafka_user_position");
    }

    private static Table getQuotationTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql("" +
                "CREATE TABLE kafka_stock_quotation (\n" +
                " stock_id STRING NOT NULL,\n" +
                " price double,\n" +
                "  kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',\n" +
                "  kafka_partition BIGINT METADATA FROM 'partition',\n" +
                "  kafka_offset BIGINT METADATA FROM 'offset',\n" +
                "  PRIMARY KEY (stock_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.quotation") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'properties.group.id' = 'user-asset',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        return tEnv.sqlQuery("select * from kafka_stock_quotation");
    }

    private static void createSinkTable(StreamTableEnvironment tEnv) {
        tEnv.executeSql("" +
                "CREATE TABLE kafka_user_asset (\n" +
                "  uid INT NOT NULL,\n" +
                "  cash_value double,\n" +
                "  position_value double,\n" +
                "  total_value double,\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '" + properties.get("mq.tb.asset") + "',\n" +
                "  'properties.bootstrap.servers' = '" + properties.get("mq.host") + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE mysql_user_asset (\n" +
                "  uid INT NOT NULL,\n" +
                "  cash_value double,\n" +
                "  position_value double,\n" +
                "  total_value double,\n" +
                "  PRIMARY KEY (`uid`) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                String.format("   'url' = 'jdbc:mysql://%s/%s',\n", properties.get("output.db.host"), properties.get("output.db.database")) +
                "   'table-name' = '" + properties.get("output.tb.asset") + "',\n" +
                "   'username' = '" + properties.get("output.db.username") + "',\n" +
                "   'password' = '" + properties.get("output.db.password") + "'\n" +
                ")" +
                "");
    }
}
