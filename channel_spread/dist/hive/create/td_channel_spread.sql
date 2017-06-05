CREATE TABLE `dwv_yzb.dwv_yzb_xk_member_device_faid` (
  `did` string COMMENT 'device id',
  `faid` string COMMENT 'idfa',
  `createtime` bigint COMMENT 'create time'
)  COMMENT 'IDFA active table'
row format delimited fields terminated by '\t'
stored as textfile;

CREATE EXTERNAL TABLE `dwv_yzb_pay_api`(
  `syslog_time` string COMMENT 'syslog���',
  `syslog_hostname` string COMMENT 'syslog���
�hhostname',
  `host` string COMMENT '
�h�
',
  `remote_addr` string COMMENT '�7�ip0@',
  `remote_user` string COMMENT '�7�(7
',
  `local_time` string COMMENT '�7���',
  `request` string COMMENT '�B0@',
  `status` int COMMENT '�',
  `body_bytes_sent` int COMMENT ' ub�W�p',
  `request_time` string COMMENT '�B��',
  `http_referer` string COMMENT '(0@',
  `http_user_agent` string COMMENT 'user agent',
  `http_x_forwarded_for` string COMMENT '�7��o:�ip,�I',
  `upstream_response_time` string COMMENT '͔pn��',
  `request_params` map<string,string> COMMENT 'secdata')
COMMENT '��/�api��(SMH,
PARTITIONED BY (
  `dt` string,
  `hour` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yixiacluster/apps/hive/warehouse/dwv_yzb.db/dwv_yzb_pay_api'
TBLPROPERTIES (
  'transient_lastDdlTime'='1495522738')