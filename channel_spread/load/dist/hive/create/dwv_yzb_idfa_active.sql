CREATE TABLE `dwv_yzb.dwv_yzb_idfa_active` (
  `faid` string COMMENT 'idfa',
  `createtime` bigint COMMENT 'create time'
)  COMMENT 'IDFA active table'
PARTITIONED BY (`app` string)
row format delimited fields terminated by '\t'
stored as textfile;