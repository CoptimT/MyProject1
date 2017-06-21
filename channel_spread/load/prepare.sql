drop table if exists temp_yzb.tmp_channel_spread_${day};

create table temp_yzb.tmp_channel_spread_${day} as
select app,spread_name,idfa from dwv_yzb.dwv_yzb_channel_spread where (dt='${day}' or (dt='${tomorrow}' and hour='00')) and day='${day}'
and app is not null and spread_name is not null and idfa!='00000000-0000-0000-0000-000000000000' and idfa is not null
group by app,spread_name,idfa;


drop table if exists temp_yzb.tmp_channel_spread_mid_${day};

create table temp_yzb.tmp_channel_spread_mid_${day} as
select t.app,t.spread_name,t.idfa,coalesce(from_unixtime(min(t.createtime),'yyyyMMdd'),'') first_date,coalesce(from_unixtime(max(t.createtime),'yyyyMMdd'),'') last_date
from (
    select b.app,b.spread_name,b.idfa,a.createtime
    from (
        select faid,createtime from dwv_yzb.dwv_yzb_xk_member_device_faid where createtime<unix_timestamp('${tomorrow}','yyyyMMdd')
    ) a join temp_yzb.tmp_channel_spread_${day} b
    on(a.faid=b.idfa)
) t group by t.app,t.spread_name,t.idfa;
