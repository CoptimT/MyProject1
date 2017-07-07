drop table if exists temp_yzb.tmp_channel_spread_${day};

create table temp_yzb.tmp_channel_spread_${day} as
select app,spread_name,idfa from dwv_yzb.dwv_yzb_channel_spread where (dt='${day}' or (dt='${tomorrow}' and hour='00')) and day='${day}'
and app is not null and spread_name is not null and idfa!='00000000-0000-0000-0000-000000000000' and idfa is not null
group by app,spread_name,idfa;

CREATE TABLE IF NOT EXISTS dwv_yzb.dwv_yzb_channel_spread_detail(
  app          string comment 'app',
  spread_name  string comment '渠道',
  idfa         string comment 'idfa',
  first_date   string comment '第一次激活日期',
  last_date    string comment '最后一次激活日期'
) partitioned by (dt string);

insert overwrite table dwv_yzb.dwv_yzb_channel_spread_detail partition (dt='${day}')
select t.app,t.spread_name,t.idfa,coalesce(from_unixtime(min(t.createtime),'yyyyMMdd'),'') first_date,coalesce(from_unixtime(max(t.createtime),'yyyyMMdd'),'') last_date
from (
    select b.app,b.spread_name,b.idfa,a.createtime
    from (
        select faid,createtime from dwv_yzb.dwv_yzb_xk_member_device_faid where createtime<unix_timestamp('${tomorrow}','yyyyMMdd')
    ) a join (
        select app,spread_name,idfa from temp_yzb.tmp_channel_spread_${day} where app='yzb'
    ) b on(a.faid=b.idfa)
    union all
    select d.app,d.spread_name,d.idfa,c.createtime
    from (
        select faid,floor(createtime/1000) as createtime from dwv_yzb.dwv_yzb_idfa_active where app='mp' and floor(createtime/1000)<unix_timestamp('${tomorrow}','yyyyMMdd')
    ) c join (
        select app,spread_name,idfa from temp_yzb.tmp_channel_spread_${day} where app='mp'
    ) d on(c.faid=d.idfa)
) t group by t.app,t.spread_name,t.idfa;
