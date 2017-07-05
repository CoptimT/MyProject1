CREATE TABLE `dwv_yzb.dwv_yzb_xk_member_device_faid` (
  `did` string COMMENT 'device id',
  `faid` string COMMENT 'idfa',
  `createtime` bigint COMMENT 'create time'
)  COMMENT 'IDFA active table'
row format delimited fields terminated by '\t'
stored as textfile;