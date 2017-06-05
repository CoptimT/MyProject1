CREATE TABLE `wallet_balance_statistic` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '差错单号',
  `balance_time` date DEFAULT NULL COMMENT '对账时间',
  `type` tinyint(1) unsigned DEFAULT '0' COMMENT '交易类型：流水对账：1 交易对账：2 资金对账 3',
  `balance_channel` varchar(64) DEFAULT '0' COMMENT '对账渠道',
  `balance_cnt` bigint(20) DEFAULT '0' COMMENT '对账总笔数',
  `balance_error_cnt` bigint(20) DEFAULT '0' COMMENT '未对平笔数',
  `createtime` bigint(13) DEFAULT '0' COMMENT '记录创建时间',
  `updatetime` bigint(13) DEFAULT '0' COMMENT '最近更新时间',
  `total_balanced_amount` bigint(20) DEFAULT '0' COMMENT '已对平金额',
  `total_unbalanced_amount` bigint(20) DEFAULT '0' COMMENT '未对平金额',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_date_type_channel` (`balance_time`,`type`,`balance_channel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;