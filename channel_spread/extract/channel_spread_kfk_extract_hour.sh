#!/bin/bash
config_dir=/home/rec/data/bigdata2/config
source /etc/profile
source /home/rec/.bash_profile
source $config_dir/hive_product_conf.sh
source $config_dir/spark_conf.sh

# 获取Kerberos票据
reckinit=$reckinit
/usr/bin/kinit -k -t $reckinit

# -----------------------------------------------------------------------------------------------------------------
# 1. 参数验证
# 1.1 过期数据设置
del_data_expire=`date -d '720 days ago' +'%Y%m%d'`
today_hour=`date +'%Y%m%d%H'`

# 1.2 如果没有传参，默认“一小时前对应的:日期、小时"
cdate=`date -d '1 hours ago' +%Y%m%d`
chour=`date -d '1 hours ago' +%H`
curr_date_hour=`date  +%Y%m%d%H`
last2h_date_hour=`date -d '2 hours ago' +%Y%m%d%H`

if [ $# -eq 2 ]
then
   cdate=$1
   chour=$2
fi
cdate_hour=$cdate$chour

echo $del_data_expire
echo $cdate
echo $chour
echo $cdate_hour
echo $curr_date_hour

# 1.3 如果输入的日期大于“数据过期日期"，并且小于“当前的日期、小时”，则运行后续脚本；否则，强制退出
if !([ $cdate -gt $del_data_expire ] && [ $cdate_hour -lt $curr_date_hour ])
then
	echo "Date parameter too long ago, The File May be not exists, Please check it first!"
	exit 1
fi

# -----------------------------------------------------------------------------------------------------------------
# 2. 数据验证
# 2.1 cd 到脚本文件所在路径
run_dir=$(dirname `readlink -f "$0"`)
cd $run_dir
echo $run_dir

# 2.2 Hive库、表及路径、Hdfs路径设置
hive_warehouse=$hive_pro01_warehouse_dir
hive_db_name=dwv_yzb
hive_tb_name=dwv_yzb_channel_spread
hdfs_source_dir=/log/source/others/td_channel_spread

run_class=com.yixia.bigdata.etl.TdChannelSpreadParser
run_extract_job_name=com.yixia.bigdata.etl.TdChannelSpreadParser.${cdate}.${chour}
run_queue=$extract_queue
run_jar=/home/rec/data/bigdata2/etl/yzb/channel_spread/jar/channel_spread.jar

# 2.3 验证当天该小时段Source源数据中 _SUCCESS 文件是否存在；
# 如果不存在，且参数非近两小时，则：强制退出运行；否则：循环30次（每次sleep 4分钟)验证 _SUCCESS 文件是否存在
# 2.3.1 验证 $hdfs_source_dir
if $(hadoop fs -test -e ${hdfs_source_dir}/${cdate}/$chour/_SUCCESS)
then
	echo "The HdfsFile: ${hdfs_source_dir}/${cdate}/${chour}/_SUCCESS Exists, Check Successful."
else
	# 如果输入参数是最近两小时，尝试循环等待判断 _SUCCESS 文件是否存在；否则：强制退出
	if  !([ $cdate_hour -gt $last2h_date_hour ] && [ $cdate_hour -lt $curr_date_hour ])
	then
		loop=0
		while true
		do
			sleep 240
			if $(hadoop fs -test -e ${hdfs_source_dir}/${cdate}/$chour/_SUCCESS)
			then
				echo echo "The HdfsFile: ${hdfs_source_dir}/${cdate}/${chour}/_SUCCESS Exists, Check Successful."
				break
			fi
			((loop++));
			if [ $loop -eq 30 ]; then
				echo "Error: Waitting For HdfsFile ${hdfs_source_dir}/${cdate}/${chour} many times, But Not Ready, Please check!"
				exit 1
			fi
		done
	else
		echo "The HdfsFile: ${hdfs_source_dir}/${cdate}/${chour}/_SUCCESS Not Exists, Please Check!"
		exit 1
	fi
fi

# -----------------------------------------------------------------------------------------------------------------
# 3. 关键代码运行
# 3.1 删除过期数据，不建议带 -skipTrash 参数，以免误操作丢失数据
if $(hadoop fs -test -d $hdfs_source_dir/$del_data_expire)
then
	hdfs  dfs -rm -r ${hdfs_source_dir}/${del_data_expire}
fi

if $(hadoop fs -test -d $hdfs_source_dir2/$del_data_expire)
then
    hdfs  dfs -rm -r ${hdfs_source_dir2}/${del_data_expire}
fi

# 3.2 删除目标路径(Hive表当天小时段的数据)，不建议带 -skipTrash 参数，以免误操作丢失数据
if $(hadoop fs -test -d  ${hive_warehouse}/${hive_db_name}.db/${hive_tb_name}/dt=${cdate}/hour=${chour})
then
	hdfs  dfs -rm -r ${hive_warehouse}/${hive_db_name}.db/${hive_tb_name}/dt=${cdate}/hour=${chour}
fi

# 3.3 extract to hive
/usr/hdp/current/spark2-client/bin/spark-submit \
--class $run_class  \
--name $run_extract_job_name \
--master yarn \
--queue $run_queue \
$run_jar $hdfs_source_dir/$cdate/$chour ${hive_warehouse}/${hive_db_name}.db/${hive_tb_name}/dt=${cdate}/hour=${chour}
if [ $? -ne 0 ]
then
    sendmessage "wechat,mail" "error" "yzb api_playback extract error(${cdate}:${chour})" "yzb api_playback extract job:${run_extract_job_name} failed!"  ${mail_extract}
  echo "ExtractError---- SourceDir:(${hdfs_source_dir})/${cdate}/${chour} TargetDir: ${hive_warehouse}/${hive_db_name}.db/${hive_tb_name}/dt=${cdate}/hour=${chour}"
  exit 1
fi

# 3.4  Add hive table partition
hive -e "use $hive_db_name; alter table $hive_tb_name add if not exists partition (dt='$cdate',hour='$chour');
         ANALYZE TABLE $hive_tb_name PARTITION(dt='$cdate',hour='$chour') COMPUTE STATISTICS;";
if [ $? -ne 0 ]
then
  echo "Add HivePartitionError. TargetDir: ${hive_warehouse}/${hive_db_name}.db/${hive_tb_name}/dt=${cdate}/hour=${chour}"
  exit 1
fi
