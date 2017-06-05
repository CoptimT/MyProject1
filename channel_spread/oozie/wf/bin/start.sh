#!/bin/bash
begin_time=`date +%s`

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $DIR/../conf/shell_env.conf

if [ ${#WORKROOT} -lt 6 ]; then
        exit 1
fi

DAY=""
#1.参数为20161225表示重跑，默认跑昨天的数据
if [ ${#1} -eq 8 ]; then
    DAY=$1
else
    DAY=`date -d -1day +%Y%m%d`
fi

#2.建立日志文件夹
if [ ! -d ${WORKROOT}/logs/${DAY} ]; then
	mkdir -p ${WORKROOT}/logs/${DAY}
fi

#3.业务库导表
echo `date`  dump mysql job begin >> ${WORKROOT}/logs/${DAY}/wf.log

dump_mysql_daily.sh ${DAY} >> ${WORKROOT}/logs/${DAY}/wf.log 2>&1

if [ $? -ne 0 ];then
    echo `date`  dump mysql job error >> ${WORKROOT}/logs/${DAY}/wf.log
    exit 1
fi

#4.oozie任务启动
echo `date`  oozie job begin >> ${WORKROOT}/logs/${DAY}/wf.log

#oozie job -oozie http://${OOZIE_SERVER}:11000/oozie/ -config ${WORKROOT}/wf/job.properties  -run -verbose -Dday=$DAY

end_time=`date +%s`
process_time=$((${end_time}-${begin_time}))
echo `date`  oozie job submited ,start job use total time ${process_time} seconds >> ${WORKROOT}/logs/${DAY}/wf.log