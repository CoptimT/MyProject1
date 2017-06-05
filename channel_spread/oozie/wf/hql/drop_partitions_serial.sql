-- 1.异常账务流水,数据导出后删除Hive分区数据
alter table yx_middle.yzb_snapshot_summary_day_balance_serialt1 drop if exists partition(day='${day}');

alter table yx_middle.yzb_snapshot_stat_day_balance_serial drop if exists partition(day='${day}');

alter table yx_middle.tmp_wallet_without_channel_serialid drop if exists partition(day='${day}');

alter table yx_middle.tmp_wallet_without_channel_balance drop if exists partition(day='${day}');


drop table yx_middle.order_realprice_${day};
drop table yx_middle.order_goldcoin_${day};
drop table yx_middle.order_diamond_${day};
drop table yx_middle.order_balance_${day};
drop table yx_middle.order_balance_channel_${day};
drop table yx_middle.order_channel_diamond_${day};