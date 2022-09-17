package asset.user_asset_snapshot.operators;

import common.pojo.UserAsset;
import common.pojo.UserAssetSnapshot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class OperatorBuilder {

    public static ProcessFunction<UserAsset, UserAssetSnapshot> doSnapshot() {
        return new ProcessFunction<UserAsset, UserAssetSnapshot>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void processElement(UserAsset asset, ProcessFunction<UserAsset, UserAssetSnapshot>.Context context, Collector<UserAssetSnapshot> collector) throws Exception {

            }
        };
    }

}
