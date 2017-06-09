
CREATE TABLE `dwv_yzb_channel_spread_statistic` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `stat_dt` date NOT NULL DEFAULT '0000-00-00' COMMENT '日期',
  `app` varchar(32) NOT NULL DEFAULT '' COMMENT '应用',
  `channel` varchar(64) NOT NULL DEFAULT '' COMMENT '渠道',
  `today_add` bigint(20) DEFAULT '0' COMMENT '今天的新增',
  `today_update` bigint(20) DEFAULT '0' COMMENT '今天的升级',
  `yesterday_add` bigint(20) DEFAULT '0' COMMENT '昨天的新增',
  `yesterday_update` bigint(20) DEFAULT '0' COMMENT '昨天的升级',
  `createtime` bigint(13) DEFAULT '0' COMMENT '记录创建时间',
  `updatetime` bigint(13) DEFAULT '0' COMMENT '最近更新时间',
  PRIMARY KEY (`id`,`stat_dt`),
  UNIQUE KEY `uk_date_app_channel` (`stat_dt`,`app`,`channel`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=FIXED COMMENT='一直播渠道推广量自动化计算'
/*!50100 PARTITION BY RANGE ( YEAR(STAT_DT))
(PARTITION P2016 VALUES LESS THAN (2017) ENGINE = InnoDB,
 PARTITION P2017 VALUES LESS THAN (2018) ENGINE = InnoDB,
 PARTITION P2018 VALUES LESS THAN (2019) ENGINE = InnoDB,
 PARTITION P2019 VALUES LESS THAN (2020) ENGINE = InnoDB,
 PARTITION P2020 VALUES LESS THAN (2021) ENGINE = InnoDB,
 PARTITION PMAX VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;