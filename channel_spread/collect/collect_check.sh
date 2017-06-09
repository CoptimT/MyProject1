#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cdate=`date -d '1 days ago' +%Y%m%d`
hdfs_source_dir=/log/source/others/td_channel_spread
if [ $# -eq 1 ]
then
   cdate=$1
fi

logfile=${DIR}/../logs/collect_check_${cdate}.log
hours="00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23"
for hour in $hours
do
  if $(hadoop fs -test -e ${hdfs_source_dir}/${cdate}/${hour}/_SUCCESS)
  then
    echo ${cdate}-${hour} ok >> ${logfile}
  else
    echo ${cdate}-${hour} failed >> ${logfile}
    sh ${DIR}/channel_spread_kfk_collect_hour.sh ${cdate} ${hour} >> ${logfile} 2>&1
  fi
done

