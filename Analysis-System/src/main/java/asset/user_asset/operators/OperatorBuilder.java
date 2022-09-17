package asset.user_asset.operators;

import common.pojo.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class OperatorBuilder {

    public static RichCoFlatMapFunction<Tuple2<Boolean, UserPosition>, StockQuotation, Tuple2<Boolean, UserPositionEtl>> enrichPositionWithQuotation() {
        return new RichCoFlatMapFunction<Tuple2<Boolean, UserPosition>, StockQuotation, Tuple2<Boolean, UserPositionEtl>>() {
            private ValueState<Double> priceState;
            private MapState<Integer, Tuple2<Boolean, UserPosition>> positionMapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("priceState", Double.class));
                positionMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("positionMapState", TypeInformation.of(Integer.class), TypeInformation.of(new TypeHint<Tuple2<Boolean, UserPosition>>() {
                })));
            }

            @Override
            public void flatMap1(Tuple2<Boolean, UserPosition> positionTuple2, Collector<Tuple2<Boolean, UserPositionEtl>> collector) throws Exception {
                // 有报价数据直接enrich持仓返回
                if (priceState.value() != null) {
                    collector.collect(enrich(positionTuple2));
                    return;
                }
                // 没有报价需要缓存持仓
                positionMapState.put(positionTuple2.f1.getUid(), positionTuple2);
            }

            @Override
            public void flatMap2(StockQuotation stockQuotation, Collector<Tuple2<Boolean, UserPositionEtl>> collector) throws Exception {
                priceState.update(stockQuotation.getPrice());
                // 如果有持仓剩余需要清空缓存
                if (!positionMapState.isEmpty()) {
                    for (Tuple2<Boolean, UserPosition> positionTuple2 : positionMapState.values()) {
                        collector.collect(enrich(positionTuple2));
                    }
                    positionMapState.clear();
                }
            }

            private Tuple2<Boolean, UserPositionEtl> enrich(Tuple2<Boolean, UserPosition> positionTuple2) throws Exception {
                Double price = priceState.value();
                return new Tuple2<>(
                        positionTuple2.f0,
                        new UserPositionEtl(
                                positionTuple2.f1.getUid(),
                                positionTuple2.f1.getStock_id(),
                                positionTuple2.f1.getQuantity(),
                                price,
                                positionTuple2.f1.getQuantity() * price
                        )
                );
            }
        };
    }

    public static RichCoFlatMapFunction<Tuple2<Boolean, UserCash>, Tuple2<Boolean, UserPositionEtl>, UserAsset> calculateUserAsset() {
        return new RichCoFlatMapFunction<Tuple2<Boolean, UserCash>, Tuple2<Boolean, UserPositionEtl>, UserAsset>() {
            private ValueState<Double> cashState;
            private ValueState<Double> positionState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                cashState = getRuntimeContext().getState(new ValueStateDescriptor<>("cashState", Double.class));
                positionState = getRuntimeContext().getState(new ValueStateDescriptor<>("positionState", Double.class));
            }

            @Override
            public void flatMap1(Tuple2<Boolean, UserCash> cashTuple2, Collector<UserAsset> collector) throws Exception {
                if (cashTuple2.f0) {
                    // dml=set
                    cashState.update(cashTuple2.f1.getCash_value());
                } else {
                    // dml=del
                    cashState.update(0.0);
                }
                collector.collect(calculateAsset(cashTuple2.f1.getUid()));
            }

            @Override
            public void flatMap2(Tuple2<Boolean, UserPositionEtl> positionEtlTuple2, Collector<UserAsset> collector) throws Exception {
                if (positionEtlTuple2.f0) {
                    // dml=set
                    positionState.update(positionEtlTuple2.f1.getPosition_value());
                } else {
                    // dml=del
                    positionState.update(0.0);
                }
                collector.collect(calculateAsset(positionEtlTuple2.f1.getUid()));
            }

            private UserAsset calculateAsset(Integer uid) throws Exception {
                double cashValue = cashState.value() == null ? 0 : cashState.value();
                double positionValue = positionState.value() == null ? 0 : positionState.value();
                return new UserAsset(
                        uid,
                        cashValue,
                        positionValue,
                        cashValue + positionValue,
                        String.format("%d%d", System.currentTimeMillis(), System.nanoTime())
                );
            }
        };
    }

}
