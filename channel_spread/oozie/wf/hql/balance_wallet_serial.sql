-- 1.查询异常账务流水
-- 1.1 查询目标账户：用户金币户，用户钻石户 当天全部流水
CREATE TABLE IF NOT EXISTS yx_middle.yzb_snapshot_summary_day_balance_serialt1 (
  id bigint comment '流水ID',
  memberid bigint comment '用户ID',
  serialid string comment '串号',
  createtime bigint comment '订单创建时间',
  firstcategory string comment '一级分类',
  secondcategory string comment '二级分类',
  goldcoin bigint comment '小咖金币',
  curgoldcoin bigint,
  diamond bigint comment '钻石数量',
  curdiamond bigint,
  sortid int
) partitioned by (day string);

insert overwrite table yx_middle.yzb_snapshot_summary_day_balance_serialt1 partition (day='${day}')
select id,memberid,serialid,createtime,firstcategory,secondcategory,nvl(goldcoin,0),nvl(curgoldcoin,0),nvl(diamond,0),nvl(curdiamond,0),
       row_number() over (partition by memberid order by createtime) as sortid
from yx_datasync.xkx_wallet_log_new where day='${day}' and (firstcategory = '0' or firstcategory='1');

-- 1.2 查询目标账户异常账务流水
CREATE TABLE IF NOT EXISTS yx_middle.yzb_snapshot_stat_day_balance_serial (
  serialid string comment '串号',
  wallet_id bigint comment '流水ID',
  balance_time bigint comment '对账时间',
  deal_time bigint comment '订单创建时间',
  firstcategory string comment '一级分类',
  secondcategory string comment '二级分类',
  memberid bigint comment '用户ID',
  goldcoin_last bigint comment '上条金币余额',
  goldcoin bigint comment '本条金币变动',
  curgoldcoin bigint comment '本条金币余额',
  goldcoin_cz bigint comment '金币差值',
  diamond_last bigint comment '上条钻石余额',
  diamond bigint comment '本条钻石变动',
  curdiamond bigint comment '本条钻石余额',
  diamond_cz bigint comment '钻石差值',
  log_date string COMMENT '交易日期'
) partitioned by (day string);

insert overwrite table yx_middle.yzb_snapshot_stat_day_balance_serial partition (day='${day}')
select serialid,wallet_id,balance_time,deal_time,firstcategory,secondcategory,memberid,goldcoin_last,goldcoin,curgoldcoin,goldcoin_cz,
       diamond_last,diamond,curdiamond,diamond_cz,log_date from (
select serialid,wallet_id,balance_time,deal_time,firstcategory,secondcategory,memberid,goldcoin_last,goldcoin,curgoldcoin,goldcoin_cz,
       diamond_last,diamond,curdiamond,diamond_cz,log_date,row_number() over (partition by memberid order by sortid) as orderid from (
select nvl(a.serialid,'') as serialid,a.id as wallet_id,nvl(unix_timestamp(),0) as balance_time,a.createtime as deal_time,
       nvl(a.firstcategory,'') as firstcategory,nvl(a.secondcategory,'') as secondcategory,nvl(a.memberid,0) as memberid,
       nvl(b.curgoldcoin,0) as goldcoin_last,nvl(a.goldcoin,0) as goldcoin,
       nvl(a.curgoldcoin,0) as curgoldcoin,nvl(b.curgoldcoin + a.goldcoin - a.curgoldcoin,0) as goldcoin_cz,
       nvl(b.curdiamond,0) as diamond_last,nvl(a.diamond,0) as diamond,
       nvl(a.curdiamond,0) as curdiamond,nvl(b.curdiamond + a.diamond - a.curdiamond,0) as diamond_cz,
       '${day}' as log_date,a.sortid from (
select * from yx_middle.yzb_snapshot_summary_day_balance_serialt1 where day='${day}' and sortid > 1
) a left join (
select * from yx_middle.yzb_snapshot_summary_day_balance_serialt1 where day='${day}'
) b on(a.memberid=b.memberid and a.sortid=b.sortid+1)
where b.id is not null and (a.curgoldcoin!=b.curgoldcoin+a.goldcoin or a.curdiamond!=b.curdiamond+a.diamond)
) c
) d where orderid=1;
