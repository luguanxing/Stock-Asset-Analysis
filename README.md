# Stock-Asset-Analysis

## 简介 Profit 
calculating realtime user_asset, user_asset tending, profit/loss trending with Flink on a simplified stock-trading system

基于一套简化的证券交易系统，使用Flink引擎计算用户实时资产、资产走势、盈亏走势



## 输入数据准备 Input Data Preparetion
业务mysql数据 mysql service data 
```
/*用户现金表*/
CREATE TABLE `tb_user_cash` (  `uid` int(64) NOT NULL,  `cash_value` double DEFAULT NULL,  PRIMARY KEY (`uid`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*用户持仓表*/
CREATE TABLE `tb_user_position` (  `uid` int(64) NOT NULL,  `stock_id` varchar(128) NOT NULL,  `quantity` double DEFAULT NULL,  PRIMARY KEY (`uid`,`stock_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

/*股票行情报价表*/
CREATE TABLE `tb_stock_quotation` (  `stock_id` varchar(128) NOT NULL,  `price` double DEFAULT NULL,  PRIMARY KEY (`stock_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*用户流水表*/
CREATE TABLE `tb_user_inout` (  `uid` int(64) NOT NULL,  `inout_type` varchar(32) NOT NULL,  `inout_value` double DEFAULT NULL,  PRIMARY KEY (`uid`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
```

## 输出数据准备 Data Outpt Preparetion
计算结果数据 calculation result data
```
CREATE TABLE `user_asset` (  `uid` int(64) NOT NULL,  `cash_value` double DEFAULT NULL,  `position_value` double DEFAULT NULL,  `total_value` double DEFAULT NULL,  PRIMARY KEY (`uid`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE `user_profit` (  `uid` int(64) NOT NULL,  `profit_value` double DEFAULT NULL,  PRIMARY KEY (`uid`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE `user_asset_snapshot` (  `uid` int(64) NOT NULL,  `ts` BIGINT DEFAULT NULL,  `asset` double DEFAULT NULL,  PRIMARY KEY (`uid`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE `user_profit_snapshot` (  `uid` int(64) NOT NULL,  `ts` BIGINT DEFAULT NULL,  `profit` double DEFAULT NULL,  PRIMARY KEY (`uid`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
```