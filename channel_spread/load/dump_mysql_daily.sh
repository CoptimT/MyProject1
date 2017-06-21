#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/shell_env.conf

DAY=$1
starttime=`date -d "${DAY}" +%s`
endtime=`date --date="${DAY} 1days" +%s`
datafile=xk_member_device_faid_${DAY}.txt

mysql -u${USERNAME} -p${PASSWORD} -h${HOST} -P${PORT} --default-character-set=utf8 --skip-column-names -B -e "select did,faid,createtime from member_xiaoka_tv.xk_member_device_faid where createtime>=$starttime and createtime<$endtime" > ${WORKROOT}/data/${datafile}

if [ $? -ne 0 ];then
    exit 1
fi

hdfs dfs -put -f ${WORKROOT}/data/${datafile} /apps/hive/warehouse/dwv_yzb.db/dwv_yzb_xk_member_device_faid/

