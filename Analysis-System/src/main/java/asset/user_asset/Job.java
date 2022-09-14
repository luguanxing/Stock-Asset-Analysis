package asset.user_asset;

import asset.user_asset.operators.OperatorBuilder;
import common.pojo.StockQuotation;
import common.pojo.UserCash;
import common.pojo.UserPosition;
import common.pojo.UserPositionEtl;
import common.utils.PropertiesUtil;
import lombok.var;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
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

        // 读取实时数仓消息队列中钱和货数据
        Table cashTable = getCashTable(stEnv);
        Table positionTable = getPositionTable(stEnv);
        Table quotationTable = getQuotationTable(stEnv);
        var cashStream = stEnv.toRetractStream(cashTable, UserCash.class);
        var positionStream = stEnv.toRetractStream(positionTable, UserPosition.class);
        var quotationStream = stEnv.toAppendStream(quotationTable, StockQuotation.class);

        // 持仓关联报价
        var positionEtlStream = positionStream
                .connect(quotationStream)
                .keyBy(
                        (KeySelector<Tuple2<Boolean, UserPosition>, String>) positionTuple -> positionTuple.f1.getStockId(),
                        (KeySelector<StockQuotation, String>) quotation -> quotation.getStockId()
                )
                .flatMap(OperatorBuilder.enrichPositionWithQuotation());

        // 计算用户资产
        var userAssetStream = cashStream
                .connect(positionEtlStream)
                .keyBy(
                        (KeySelector<Tuple2<Boolean, UserCash>, Integer>) cashTuple -> cashTuple.f1.getUid(),
                        (KeySelector<Tuple2<Boolean, UserPositionEtl>, Integer>) positionTuple -> positionTuple.f1.getUid()
                )
                .flatMap(OperatorBuilder.calculateUserAsset());

        // 数据结果保存到mysql和kafka

        // 输出结果
        env.execute("User Asset");
    }

    private static Table getCashTable(StreamTableEnvironment tEnv) {

    }

    private static Table getPositionTable(StreamTableEnvironment tEnv) {

    }

    private static Table getQuotationTable(StreamTableEnvironment tEnv) {

    }

}
