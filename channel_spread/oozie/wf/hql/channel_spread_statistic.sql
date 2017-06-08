-- 1.结果统计

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


insert overwrite table dwv_yzb.dwv_yzb_channel_spread_statistic partition (dt='${day}')
select '${day}',app,spread_name,sum(today_add) today_add,sum(today_update) today_update,
       sum(yesterday_add) yesterday_add,sum(yesterday_update) yesterday_update,
       concat(nvl(unix_timestamp(),0),'000'),concat(nvl(unix_timestamp(),0),'000') from (

  --a.最新出现时间为当天且只出现了一次的idfa数量，则统计为今天的新增
  select app,spread_name,count(distinct idfa) today_add,0 today_update,0 yesterday_add,0 yesterday_update
  from temp_yzb.tmp_channel_spread_mid_${day} where cnt=1 and last_date='${day}' group by app,spread_name
union all
  --b.最新出现时间为当天且出现次数>1的idfa数量，则统计为今天的升级
  select app,spread_name,0 today_add,count(distinct idfa) today_update,0 yesterday_add,0 yesterday_update
  from temp_yzb.tmp_channel_spread_mid_${day} where cnt>1 and last_date='${day}' group by app,spread_name
union all
  --c.最新出现时间为昨天且只出现了一次的idfa数量，则统计为昨天的新增
  select app,spread_name,0 today_add,0 today_update,count(distinct idfa) yesterday_add,0 yesterday_update
  from temp_yzb.tmp_channel_spread_mid_${day} where cnt=1 and last_date='${yesterday}' group by app,spread_name
union all
  --d.最新出现的时间为昨天且出现次数>1的idfa数量，则统计为昨天的升级
  select app,spread_name,0 today_add,0 today_update,0 yesterday_add,count(distinct idfa) yesterday_update
  from temp_yzb.tmp_channel_spread_mid_${day} where cnt>1 and last_date='${yesterday}' group by app,spread_name

) tb group by app,spread_name;

