CREATE EXTERNAL TABLE `dwv_yzb.dwv_yzb_channel_spread`(
  app string,
  eventtime string,
  idfa string,
  spreadname string,
  ip string,
  devicetype string,
  appkey string,
  clickip string,
  clicktime string,
  adnetname string,
  tdid string,
  ua string)
PARTITIONED BY (`dt` string,`hour` string)
stored as parquet
LOCATION
  'hdfs://yixiacluster/apps/hive/warehouse/dwv_yzb.db/dwv_yzb_channel_spread';