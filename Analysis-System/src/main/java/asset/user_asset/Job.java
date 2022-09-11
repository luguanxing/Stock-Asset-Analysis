package asset.user_asset;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {

    public static void main(String[] args) throws Exception {
        // 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据集
        env.socketTextStream("127.0.0.1", 12345)
                .map((MapFunction<String, String>) s -> "[" + s + "]")
                .print();

        // 输出结果
        env.execute("print");
    }

}
