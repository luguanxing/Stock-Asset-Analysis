package asset.cdc;

import common.utils.PropertiesUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Job {
    // 获取配置文件
    private static final ParameterTool properties = PropertiesUtil.getProperties();

    public static void main(String[] args) throws Exception {
        // 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration().set(RestOptions.PORT, properties.getInt("flink.webui.cdc")));
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        StatementSet statementSet = tEnv.createStatementSet();

        // 读取业务数据，同步到实时数仓消息队列中
        tEnv.executeSql("" +
                "CREATE TABLE mysql_user_cash (\n" +
                " uid INT NOT NULL,\n" +
                " cash_value double\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '" + properties.get("input.db.host") + "',\n" +
                " 'port' = '" + properties.get("input.db.port") + "',\n" +
                " 'username' = '" + properties.get("input.db.username") + "',\n" +
                " 'password' = '" + properties.get("input.db.password") + "',\n" +
                " 'database-name' = '" + properties.get("input.db.database") + "',\n" +
                " 'table-name' = '" + properties.get("input.tb.cash") + "'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE mysql_user_position (\n" +
                " uid INT NOT NULL,\n" +
                " stock_id STRING NOT NULL,\n" +
                " quantity double\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '" + properties.get("input.db.host") + "',\n" +
                " 'port' = '" + properties.get("input.db.port") + "',\n" +
                " 'username' = '" + properties.get("input.db.username") + "',\n" +
                " 'password' = '" + properties.get("input.db.password") + "',\n" +
                " 'database-name' = '" + properties.get("input.db.database") + "',\n" +
                " 'table-name' = '" + properties.get("input.tb.position") + "'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE mysql_stock_quotation (\n" +
                " stock_id STRING NOT NULL,\n" +
                " price double\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '" + properties.get("input.db.host") + "',\n" +
                " 'port' = '" + properties.get("input.db.port") + "',\n" +
                " 'username' = '" + properties.get("input.db.username") + "',\n" +
                " 'password' = '" + properties.get("input.db.password") + "',\n" +
                " 'database-name' = '" + properties.get("input.db.database") + "',\n" +
                " 'table-name' = '" + properties.get("input.tb.quotation") + "'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE mysql_user_inout (\n" +
                " id INT NOT NULL,\n" +
                " uid INT NOT NULL,\n" +
                " inout_type STRING,\n" +
                " inout_value double\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '" + properties.get("input.db.host") + "',\n" +
                " 'port' = '" + properties.get("input.db.port") + "',\n" +
                " 'username' = '" + properties.get("input.db.username") + "',\n" +
                " 'password' = '" + properties.get("input.db.password") + "',\n" +
                " 'database-name' = '" + properties.get("input.db.database") + "',\n" +
                " 'table-name' = '" + properties.get("input.tb.inout") + "'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE kafka_user_cash (\n" +
                "  uid INT NOT NULL,\n" +
                "  cash_value double,\n" +
                "  PRIMARY KEY (uid) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ properties.get("mq.tb.cash") + "',\n" +
                "  'properties.bootstrap.servers' = '"+ properties.get("mq.host") + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE kafka_user_position (\n" +
                " uid INT NOT NULL,\n" +
                " stock_id STRING NOT NULL,\n" +
                " quantity double,\n" +
                "  PRIMARY KEY (uid, stock_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ properties.get("mq.tb.position") + "',\n" +
                "  'properties.bootstrap.servers' = '"+ properties.get("mq.host") + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE kafka_stock_quotation (\n" +
                " stock_id STRING NOT NULL,\n" +
                " price double,\n" +
                "  PRIMARY KEY (stock_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ properties.get("mq.tb.quotation") + "',\n" +
                "  'properties.bootstrap.servers' = '"+ properties.get("mq.host") + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");
        tEnv.executeSql("" +
                "CREATE TABLE kafka_user_inout (\n" +
                " id INT NOT NULL,\n" +
                " uid INT NOT NULL,\n" +
                " inout_type STRING,\n" +
                " inout_value double,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = '"+ properties.get("mq.tb.inout") + "',\n" +
                "  'properties.bootstrap.servers' = '"+ properties.get("mq.host") + "',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")" +
                "");



        statementSet.addInsertSql("insert into kafka_user_cash select * from mysql_user_cash");
        statementSet.addInsertSql("insert into kafka_user_position select * from mysql_user_position");
        statementSet.addInsertSql("insert into kafka_stock_quotation select * from mysql_stock_quotation");
        statementSet.addInsertSql("insert into kafka_user_inout select * from mysql_user_inout");

        statementSet.execute();
    }

}
