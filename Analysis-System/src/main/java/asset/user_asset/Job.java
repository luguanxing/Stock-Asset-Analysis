package asset.user_asset;

import common.utils.PropertiesUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {
    // 获取配置文件
    private static ParameterTool properties = PropertiesUtil.getProperties();

    public static void main(String[] args) throws Exception {
        // 初始化环境
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, properties.getInt("flink.webui.user_asset"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        // 读取实时数仓消息队列中钱和货数据，进行资产计算


        // 输出结果
        env.execute("User Asset");
    }

}
