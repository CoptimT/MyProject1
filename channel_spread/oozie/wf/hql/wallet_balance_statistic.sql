-- 1.交易结果统计

CREATE TABLE IF NOT EXISTS yx_middle.tmp_wallet_balance_statistic(
  balance_time date comment '对账时间',
  type tinyint comment '交易类型：流水对账：1 交易对账：2 资金对账 3',
  balance_channel string comment '渠道',
  balance_cnt bigint comment '对账总笔数',
  balance_error_cnt bigint comment '未对平笔数',
  createtime bigint comment '记录创建时间',
  updatetime bigint comment '最近更新时间'
) partitioned by (day string);



-- 2. 对账结果统计(渠道充值除外的对账结果)
insert overwrite table yx_middle.tmp_wallet_balance_statistic partition (day='${day}')
select from_unixtime(unix_timestamp('${day}', 'yyyyMMdd'),'yyyy-MM-dd') balance_time,type,0 balance_channel,sum(balance_cnt),sum(balance_error_cnt),
 concat(nvl(unix_timestamp(),0),'000'),concat(nvl(unix_timestamp(),0),'000') from (
select '2' type,count(distinct serialid) balance_cnt,0 balance_error_cnt  from yx_middle.tmp_wallet_without_channel_serialid where day='${day}'
union all
select '2' type,0 balance_cnt,count(1) balance_error_cnt from yx_middle.tmp_wallet_without_channel_balance where day='${day}'
union all
select '1' type,count(1) balance_cnt,0 balance_error_cnt from yx_middle.yzb_snapshot_summary_day_balance_serialt1 where day='${day}'
union all
select '1' type,0 balance_cnt,count(1) balance_error_cnt from yx_middle.yzb_snapshot_stat_day_balance_serial where day='${day}'
union all
select '2' type,0 balance_cnt,count(1) balance_error_cnt from yx_middle.order_balance_${day}
union all
select '2' type,0 balance_cnt,count(1) balance_error_cnt from yx_middle.order_balance_channel_${day}
) tb group by type;







