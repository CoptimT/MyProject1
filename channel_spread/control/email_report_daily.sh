#!/bin/bash
config_dir=/home/rec/data/bigdata2/config
source /etc/profile
source /home/rec/.bash_profile
source $config_dir/hive_product_conf.sh
source $config_dir/spark_conf.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 1. date参数
cdate=`date -d '1 days ago' +%Y%m%d`
if [ $# -eq 1 ];then
   cdate=$1
fi

email_content=${DIR}/email_content.txt
logfile=${DIR}/email_report_daily.log
echo "=========================================<br>" > ${email_content}
echo `date '+%Y-%m-%d %H:%M:%S'` cdate=${cdate} >> ${email_content}
echo "<br>=========================================<br>" >> ${email_content}
# 2. 自定义项目检查
pro_dir=/home/rec/data/bigdata2/etl/yzb/channel_spread
sh ${DIR}/check_project.sh ${cdate} ${pro_dir} >> ${email_content}

pro_dir=/home/rec/data/bigdata2/etl/mp/api
sh ${DIR}/check_project.sh ${cdate} ${pro_dir} >> ${email_content}

pro_dir=/home/rec/data/bigdata2/etl/mp/server
sh ${DIR}/check_project.sh ${cdate} ${pro_dir} >> ${email_content}

sendmessage "mail" "info" "项目运行报告" "`more ${email_content}`" zhangxiangwei

cat ${email_content} >> ${logfile}
