CREATE TABLE IF NOT EXISTS dwv_yzb.dwv_yzb_channel_spread_statistic(
  day          string comment '日期',
  app          string comment 'app',
  spread_name  string comment '渠道',
  today_add    bigint comment '今天的新增',
  today_update bigint comment '今天的升级',
  yesterday_add    bigint comment '昨天的新增',
  yesterday_update bigint comment '昨天的升级',
  createtime bigint comment '记录创建时间',
  updatetime bigint comment '最近更新时间'
) partitioned by (dt string)
stored as textfile;