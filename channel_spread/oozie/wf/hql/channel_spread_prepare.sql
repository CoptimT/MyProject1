-- 存在问题
-- 1.表分区的第一个小时00存在前一天的数据，因为Kafka时间戳延迟于实际
-- 2.一个IDFA一天中可能出现多次
-- 现象如下:
-- select day,count(1),count(distinct idfa) from dwv_yzb_channel_spread where dt='20170603' and hour='00' group by day;
--+-----------+------+------+--+
--|    day    |  c1  |  c2  |
--+-----------+------+------+--+
--| 20170602  | 33   | 32   |
--| 20170603  | 776  | 718  |
--+-----------+------+------+--+
-- 解决
-- 1.任务定时需在凌晨1点后
-- 2.先将IDFA去重再关联查询


-- 1.数据准备
-- 1.1 查询当天去重IDFA记录

-- 1.1.1 删除中间临时表
drop table if exists temp_yzb.tmp_channel_spread_${day};
-- 1.1.2 执行查询
create table temp_yzb.tmp_channel_spread_${day} as
select idfa,spread_name from dwv_yzb.dwv_yzb_channel_spread where (dt='${day}' or (dt='${tomorrow}' and hour='00')) and day='${day}' group by idfa,spread_name;


-- 1.2 关联查询IDFA出现次数和最新出现时间

-- 1.2.1 删除中间临时表
drop table if exists temp_yzb.tmp_channel_spread_mid_${day};
-- 1.2.2 执行查询
create table temp_yzb.tmp_channel_spread_mid_${day} as
select t.app,t.spread_name,t.idfa,count(1) cnt,max(t.createtime) last_date from (
    select b.app,b.idfa,b.spread_name,a.createtime from dwv_yzb.dwv_yzb_xk_member_device_faid a
    left join temp_yzb.tmp_channel_spread_${day} b on(a.faid=b.idfa)
) t group by t.app,t.spread_name,t.idfa;

