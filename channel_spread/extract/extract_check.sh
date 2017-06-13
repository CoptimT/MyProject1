#!/bin/bash
config_dir=/home/rec/data/bigdata2/config
source /etc/profile
source /home/rec/.bash_profile
source $config_dir/hive_product_conf.sh
source $config_dir/spark_conf.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Hive库、表及路径、Hdfs路径设置
hive_warehouse=$hive_pro01_warehouse_dir
hive_db_name=dwv_yzb
hive_tb_name=dwv_yzb_channel_spread
cdate=`date -d '1 days ago' +%Y%m%d`
if [ $# -eq 1 ]
then
   cdate=$1
fi

logfile=${DIR}/../logs/extract_check_${cdate}.log
hours="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23"
for hour in $hours
do
  if $(hadoop fs -test -e ${hive_warehouse}/${hive_db_name}.db/${hive_tb_name}/dt=${cdate}/hour=${hour}/_SUCCESS)
  then
    echo ${cdate}-${hour} ok >> ${logfile}
  else
    echo ${cdate}-${hour} failed >> ${logfile}
    sh ${DIR}/channel_spread_kfk_extract_hour.sh ${cdate} ${hour} >> ${logfile} 2>&1
  fi
done

