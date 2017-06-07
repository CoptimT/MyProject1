CREATE TABLE `dwv_yzb_channel_spread_statistic` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `day` varchar(8) DEFAULT NULL COMMENT '日期',
  `app` varchar(32) DEFAULT NULL COMMENT '应用',
  `spread_name` varchar(64) DEFAULT NULL COMMENT '渠道',
  `today_add` bigint(20) DEFAULT '0' COMMENT '今天的新增',
  `today_update` bigint(20) DEFAULT '0' COMMENT '今天的升级',
  `yesterday_add` bigint(20) DEFAULT '0' COMMENT '昨天的新增',
  `yesterday_update` bigint(20) DEFAULT '0' COMMENT '昨天的升级',
  `createtime` bigint(13) DEFAULT '0' COMMENT '记录创建时间',
  `updatetime` bigint(13) DEFAULT '0' COMMENT '最近更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_date_app_channel` (`day`,`app`,`spread_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;