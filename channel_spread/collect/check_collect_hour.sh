#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cdate=`date -d '2 hours ago' +%Y%m%d`
chour=`date -d '2 hours ago' +%H`
hdfs_source_dir=/log/source/others/td_channel_spread

logfile=${DIR}/../job/check_collect_${cdate}.log
echo ---------------------------------------------- >> ${logfile}
echo `date '+%Y-%m-%d %H:%M:%S'` cdate=${cdate}, chour=${chour} >> ${logfile}
echo ---------------------------------------------- >> ${logfile}

hours="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23"
for hour in $hours
do
  if [ ${hour} -le ${chour} ];then
    if $(hadoop fs -test -e ${hdfs_source_dir}/${cdate}/${hour}/_SUCCESS)
    then
      echo ${cdate}-${hour} ok >> ${logfile}
    else
      echo ${cdate}-${hour} failed >> ${logfile}
      sh ${DIR}/channel_spread_kfk_collect_hour.sh ${cdate} ${hour} >> ${logfile} 2>&1
    fi
  fi
done
echo `date '+%Y-%m-%d %H:%M:%S'` check finish. >> ${logfile}

