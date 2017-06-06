CREATE EXTERNAL TABLE `dwv_yzb.dwv_yzb_channel_spread`(
  day string,
  app string,
  event_time string,
  idfa string,
  spread_name string,
  ip string,
  device_type string,
  app_key string,
  click_ip string,
  click_time string,
  adnet_name string,
  tdid string,
  ua string)
PARTITIONED BY (`dt` string,`hour` string)
stored as parquet
LOCATION
  'hdfs://yixiacluster/apps/hive/warehouse/dwv_yzb.db/dwv_yzb_channel_spread';