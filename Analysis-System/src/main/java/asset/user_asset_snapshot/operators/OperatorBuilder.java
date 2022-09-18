package asset.user_asset_snapshot.operators;

import common.pojo.UserAsset;
import common.pojo.UserAssetSnapshot;
import lombok.var;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class OperatorBuilder {

    public static ProcessFunction<UserAsset, UserAssetSnapshot> doSnapshot() {
        return new ProcessFunction<UserAsset, UserAssetSnapshot>() {
            private ValueState<UserAssetSnapshot> snapshotState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                snapshotState = getRuntimeContext().getState(new ValueStateDescriptor<>("assetState", UserAssetSnapshot.class));
            }

            @Override
            public void processElement(UserAsset asset, ProcessFunction<UserAsset, UserAssetSnapshot>.Context context, Collector<UserAssetSnapshot> collector) throws Exception {
                var snapshot = new UserAssetSnapshot(asset);
                collector.collect(snapshot);
                // 启动定时器定时输出
                if (snapshotState.value() == null) {
                    context.timerService().registerProcessingTimeTimer(System.currentTimeMillis());
                    snapshotState.update(snapshot);
                }
                // state中只保存更晚的数据
                if (snapshotState.value().getKakfa_offset() < asset.getKafka_offset()) {
                    snapshotState.update(snapshot);
                }
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<UserAsset, UserAssetSnapshot>.OnTimerContext ctx, Collector<UserAssetSnapshot> collector) throws Exception {
                // 每分钟进行触发输出
                ctx.timerService().registerProcessingTimeTimer(timestamp + 60 * 1000);
                // Timer触发时间小于等于有记录的asset的最新update_time，否则说明数据有被更新过或定时器触发延迟
                var snapshot = snapshotState.value();
                if (timestamp <= snapshot.getUpdate_time()) {
                    return;
                }
                snapshot.setUpdate_time(roundMinuteTs(timestamp));
                collector.collect(snapshot);
            }

            private Long roundMinuteTs(long ts) {
                LocalDateTime localDateTime = new Date(ts).toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();
                localDateTime = localDateTime.truncatedTo(ChronoUnit.MINUTES);
                return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
            }
        };
    }

}
