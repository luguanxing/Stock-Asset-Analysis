package asset.user_asset.operators;

import common.pojo.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class OperatorBuilder {

    public static RichCoFlatMapFunction<Tuple2<Boolean, UserPosition>, StockQuotation, Tuple2<Boolean, UserPositionEtl>> enrichPositionWithQuotation() {
        return new RichCoFlatMapFunction<Tuple2<Boolean, UserPosition>, StockQuotation, Tuple2<Boolean, UserPositionEtl>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void flatMap1(Tuple2<Boolean, UserPosition> positionTuple2, Collector<Tuple2<Boolean, UserPositionEtl>> collector) throws Exception {

            }

            @Override
            public void flatMap2(StockQuotation stockQuotation, Collector<Tuple2<Boolean, UserPositionEtl>> collector) throws Exception {

            }
        };
    }

    public static RichCoFlatMapFunction<Tuple2<Boolean, UserCash>, Tuple2<Boolean, UserPositionEtl>, UserAsset> calculateUserAsset() {
        return new RichCoFlatMapFunction<Tuple2<Boolean, UserCash>, Tuple2<Boolean, UserPositionEtl>, UserAsset>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void flatMap1(Tuple2<Boolean, UserCash> booleanUserCashTuple2, Collector<UserAsset> collector) throws Exception {

            }

            @Override
            public void flatMap2(Tuple2<Boolean, UserPositionEtl> booleanUserPositionEtlTuple2, Collector<UserAsset> collector) throws Exception {

            }
        };
    }

}
