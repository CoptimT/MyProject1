#!/bin/bash
cdate=$1
pro_dir=$2

stat=`tail -28 ${pro_dir}/job/check_extract_${cdate}.log |grep ${cdate}|grep ok|wc -l`
if [ ${stat} -ge 24 ];then
  echo "<font color=red>${pro_dir} success</font><br/>"
else
  echo "<font color=red>${pro_dir} fail</font><br/>"
fi
tail -28 ${pro_dir}/job/check_extract_${cdate}.log | sed 's/$/<br>/g'
echo "=========================================<br/>"
