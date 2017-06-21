#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${DIR}/shell_env.conf

if [ ${#WORKROOT} -lt 6 ]; then
    exit 1
fi

#1.参数为20161225表示重跑，默认跑昨天的数据
DAY=`date -d -1day +%Y%m%d`
must_run=0
if [ ${#1} -eq 8 ]; then
    DAY=$1
fi
if [ $# -eq 2 ]; then
  must_run=$2
fi
day=$DAY
yesterday=`date --date="${DAY} -1days" +%Y%m%d`
tomorrow=`date --date="${DAY} 1days" +%Y%m%d`

#2.check
logfile=${WORKROOT}/job/job_${DAY}.log
echo ----------------------------------------------------------------------------- >> ${logfile}
echo "start job with date=${DAY} yesterday=${yesterday} tomorrow=${tomorrow} must_run=${must_run}" >> ${logfile}
echo ----------------------------------------------------------------------------- >> ${logfile}
if [ ${must_run} -eq 0 ];then
  if [ -f ${logfile} ];then
    status=`more ${logfile}|grep "channel_spread finish"|wc -l`
    if [ ${status} -gt 0 ];then
      echo `date +'%Y-%m-%d %H:%M:%S'` job have finish,not running again. >> ${logfile}
      exit 0
    fi
  fi
fi

#3.业务库导表
echo `date +'%Y-%m-%d %H:%M:%S'`  dump mysql job begin >> ${logfile}

sh ${WORKROOT}/load/dump_mysql_daily.sh ${DAY} >> ${logfile} 2>&1

if [ $? -ne 0 ];then
    echo `date +'%Y-%m-%d %H:%M:%S'` dump mysql job error >> ${logfile}
    exit 1
fi
echo `date +'%Y-%m-%d %H:%M:%S'` dump mysql job finish >> ${logfile}

#4.任务启动
echo `date +'%Y-%m-%d %H:%M:%S'` job begin >> ${logfile}
#------------
if [ -f ${DIR}/prepare_${day}.sql ];then
  rm -rf ${DIR}/prepare_${day}.sql
fi
if [ -f ${DIR}/statistic_${day}.sql ];then
  rm -rf ${DIR}/statistic_${day}.sql
fi
if [ -f ${DIR}/drop_${day}.sql ];then
  rm -rf ${DIR}/drop_${day}.sql
fi
echo `date +'%Y-%m-%d %H:%M:%S'`  delete sql file if exist finish >> ${logfile}
#------------
cp ${DIR}/prepare.sql ${DIR}/prepare_${day}.sql
cp ${DIR}/statistic.sql ${DIR}/statistic_${day}.sql
cp ${DIR}/drop.sql ${DIR}/drop_${day}.sql

sed -i "s/\${day}/${day}/g" ${DIR}/prepare_${day}.sql
sed -i "s/\${tomorrow}/${tomorrow}/g" ${DIR}/prepare_${day}.sql

sed -i "s/\${day}/${day}/g" ${DIR}/statistic_${day}.sql
sed -i "s/\${yesterday}/${yesterday}/g" ${DIR}/statistic_${day}.sql

sed -i "s/\${day}/${day}/g" ${DIR}/drop_${day}.sql
echo `date +'%Y-%m-%d %H:%M:%S'`  create sql file finish >> ${logfile}
#------------
hive -f ${DIR}/prepare_${day}.sql >> ${logfile} 2>&1
if [ $? -ne 0 ];then
  echo `date +'%Y-%m-%d %H:%M:%S'`  run prepare job error >> ${logfile}
  exit 1
fi
echo `date +'%Y-%m-%d %H:%M:%S'`  run hive prepare job ok >> ${logfile}
#------------
hive -f ${DIR}/statistic_${day}.sql >> ${logfile} 2>&1
if [ $? -ne 0 ];then
  echo `date +'%Y-%m-%d %H:%M:%S'`  run statistic job error >> ${logfile}
  exit 1
fi
echo `date +'%Y-%m-%d %H:%M:%S'`  run hive statistic job ok >> ${logfile}
#------------
hive -f ${DIR}/drop_${day}.sql >> ${logfile} 2>&1
if [ $? -ne 0 ];then
  echo `date +'%Y-%m-%d %H:%M:%S'`  run drop job error >> ${logfile}
  exit 1
fi
echo `date +'%Y-%m-%d %H:%M:%S'`  run hive drop job ok >> ${logfile}
echo `date +'%Y-%m-%d %H:%M:%S'`  run all hive job ok >> ${logfile}
#
if [ -f ${DIR}/prepare_${day}.sql ];then
  rm -rf ${DIR}/prepare_${day}.sql
fi
if [ -f ${DIR}/statistic_${day}.sql ];then
  rm -rf ${DIR}/statistic_${day}.sql
fi
if [ -f ${DIR}/drop_${day}.sql ];then
  rm -rf ${DIR}/drop_${day}.sql
fi
echo `date +'%Y-%m-%d %H:%M:%S'`  delete sql file finish >> ${logfile}

sqoop export -Dorg.apache.sqoop.export.text.dump_data_on_error=true --connect ${jdbcRes} --username ${usernameRes} --password ${passwordRes} --table dwv_yzb_channel_spread_statistic --columns stat_dt,app,channel,today_add,today_update,yesterday_add,yesterday_update,createtime,updatetime -m 1 --export-dir ${hivehouse}/dwv_yzb.db/dwv_yzb_channel_spread_statistic/dt=${day} --input-fields-terminated-by '\001' --update-key stat_dt,app,channel --update-mode allowinsert >> ${logfile} 2>&1
if [ $? -ne 0 ];then
  echo `date +'%Y-%m-%d %H:%M:%S'`  "sqoop export job error" >> ${logfile}
  exit 1
fi
echo `date +'%Y-%m-%d %H:%M:%S'`  "sqoop export job ok" >> ${logfile}

echo `date +'%Y-%m-%d %H:%M:%S'`  channel_spread finish >> ${logfile}

