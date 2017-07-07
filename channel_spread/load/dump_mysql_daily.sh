#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/shell_env.conf

DAY=$1
starttime=`date -d "${DAY}" +%s`
endtime=`date --date="${DAY} 1days" +%s`

datafile=xk_member_device_faid_${DAY}.txt
mp_file=promote_${DAY}.txt

# 1.yzb
mysql -u${USERNAME} -p${PASSWORD} -h${HOST} -P${PORT} --default-character-set=utf8 --skip-column-names -B -e "select did,faid,createtime from member_xiaoka_tv.xk_member_device_faid where createtime>=$starttime and createtime<$endtime" > ${WORKROOT}/data/${datafile}

if [ $? -ne 0 ];then
    echo "dump yzb business table fail."
    exit 1
fi
echo "dump yzb business table ok."

# 2.mp
mysql -u${MP_USERNAME} -p${MP_PASSWORD} -h${MP_HOST} -P${MP_PORT} --default-character-set=utf8 --skip-column-names -B -e "select udid,activateTime from yixia.promote where activateTime>=${starttime}000 and activateTime<${endtime}000" > ${WORKROOT}/data/${mp_file}

if [ $? -ne 0 ];then
    echo "dump mp business table fail."
    exit 1
fi
echo "dump mp business table ok."

hdfs dfs -put -f ${WORKROOT}/data/${datafile} /apps/hive/warehouse/dwv_yzb.db/dwv_yzb_xk_member_device_faid/
if [ $? -ne 0 ];then
    echo "hdfs put yzb business table fail."
    exit 1
fi
echo "hdfs put yzb business table ok."

hdfs dfs -put -f ${WORKROOT}/data/${mp_file} /apps/hive/warehouse/dwv_yzb.db/dwv_yzb_idfa_active/app=mp/
if [ $? -ne 0 ];then
    echo "hdfs put mp business table fail."
    exit 1
fi
echo "hdfs put mp business table ok."
