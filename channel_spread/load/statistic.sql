insert overwrite table dwv_yzb.dwv_yzb_channel_spread_statistic partition (dt='${day}')
select from_unixtime(unix_timestamp('${day}', 'yyyyMMdd'),'yyyy-MM-dd') day,
       app,spread_name,
       sum(today_add) today_add,
       sum(today_update) today_update,
       sum(yesterday_add) yesterday_add,
       sum(yesterday_update) yesterday_update,
       nvl(unix_timestamp(),0),nvl(unix_timestamp(),0) from (

  select app,spread_name,count(distinct idfa) today_add,0 today_update,0 yesterday_add,0 yesterday_update
  from dwv_yzb.dwv_yzb_channel_spread_detail where dt='${day}' and first_date='${day}' group by app,spread_name
union all
  select app,spread_name,0 today_add,count(distinct idfa) today_update,0 yesterday_add,0 yesterday_update
  from dwv_yzb.dwv_yzb_channel_spread_detail where dt='${day}' and app='yzb' and last_date='${day}' and last_date!=first_date group by app,spread_name
union all
  select app,spread_name,0 today_add,0 today_update,count(distinct idfa) yesterday_add,0 yesterday_update
  from dwv_yzb.dwv_yzb_channel_spread_detail where dt='${day}' and app='yzb' and first_date='${yesterday}' and first_date=last_date group by app,spread_name
union all
  select app,spread_name,0 today_add,0 today_update,0 yesterday_add,count(distinct idfa) yesterday_update
  from dwv_yzb.dwv_yzb_channel_spread_detail where dt='${day}' and app='yzb' and last_date='${yesterday}' and last_date!=first_date group by app,spread_name

) tb group by app,spread_name;
