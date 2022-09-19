package asset.user_profit.opertators;

import common.pojo.UserAsset;
import common.pojo.UserInout;
import common.pojo.UserProfit;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class OperatorBuilder {

    public static RichCoFlatMapFunction<UserAsset, UserInout, UserProfit> calculateProfit() {
        return new RichCoFlatMapFunction<UserAsset, UserInout, UserProfit>() {
            ValueState<Double> assetState;
            MapState<Integer, Double> inoutMapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                assetState = getRuntimeContext().getState(new ValueStateDescriptor<>("assetState", Double.class));
                inoutMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("inoutMapState", Integer.class, Double.class));
            }

            @Override
            public void flatMap1(UserAsset userAsset, Collector<UserProfit> collector) throws Exception {
                assetState.update(userAsset.getTotal_value());
                collector.collect(calculateProfit(userAsset.getUid()));
            }

            @Override
            public void flatMap2(UserInout userInout, Collector<UserProfit> collector) throws Exception {
                inoutMapState.put(userInout.getId(), userInout.getInout_value());
                collector.collect(calculateProfit(userInout.getUid()));
            }

            private UserProfit calculateProfit(Integer uid) throws Exception {
                // profit = asset - sum(inout)
                double asset = assetState.value() == null ? 0.0 : assetState.value();
                double inoutSum = 0;
                for (double inout : inoutMapState.values()) {
                    inoutSum += inout;
                }
                double profit = asset - inoutSum;
                return new UserProfit(uid, profit, String.format("%d%d", System.currentTimeMillis(), System.nanoTime()));
            }
        };
    }


}
