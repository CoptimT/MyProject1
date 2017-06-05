#!/bin/bash
config_dir=/home/rec/data/bigdata2/config
source /etc/profile
source /home/rec/.bash_profile
source $config_dir/hive_product_conf.sh
source $config_dir/spark_conf.sh

# 获取Kerberos票据
reckinit=$reckinit
/usr/bin/kinit -k -t $reckinit
# -----------------------------------------------------------------------------------------------------------------
# 1. 参数验证
# 1.1 过期数据设置
del_data_expire=`date -d '14 days ago' +'%Y%m%d'`
today_hour=`date +'%Y%m%d%H'`

# 1.2 如果没有传参，默认“一小时前对应的:日期、小时"
cdate=`date -d '1 hours ago' +%Y%m%d`
chour=`date -d '1 hours ago' +%H`
must_run=0
curr_date_hour=`date +'%Y%m%d%H'`

if [ $# -eq 2 ]
then
   cdate=$1
   chour=$2
fi
if [ $# -eq 3 ]
then
   cdate=$1
   chour=$2
   must_run=$3
fi
cdate_hour=$cdate$chour

echo $del_data_expire
echo $cdate
echo $chour
echo $must_run
echo $cdate_hour

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
kafka_offset_dir=/log/config/kafka/offset/td_channel_spread
hdfs_source_dir=/log/source/others/td_channel_spread
kafka_topic=talkingdata

run_offset_class=com.mp.kafka.GetOffsetByTimeStamp
run_offset_job_name=com.mp.kafka.GetOffset.tdChannel_GetOffsetByTimeStamp.$cdate.$chour
run_offset_queue=$collect_queue
run_offset_jar=/home/rec/data/bigdata2/etl/yzb/channel_spread/jar/kafkaOffset-assembly-1.1.jar

run_collect_class=com.mp.kafka.GetMessageByTimeStamp
run_collect_job_name=com.mp.kafka.Kafka2hdfs.tdChannel_GetMessageByTimeStamp.$cdate.$chour
run_collect_queue=$collect_queue
run_collect_jar=/home/rec/data/bigdata2/etl/yzb/channel_spread/jar/kafka2hdfs.jar

# -----------------------------------------------------------------------------------------------------------------
# 3. 关键代码运行
# 3.1 delete and create kafka offset
if $(hadoop fs -test -e ${kafka_offset_dir}/${cdate}/$chour/_SUCCESS)
then
    hdfs dfs -rm -r ${kafka_offset_dir}/${cdate}/$chour
fi

if $(hadoop fs -test -e ${kafka_offset_dir}/_BEGINRUNNING.${cdate}.${chour})
then
	echo "Then file ${kafka_offset_dir}/_BEGINRUNNING.${cdate}.${chour} Exists, MayBe another process are running!"
else
		hdfs dfs -touchz ${kafka_offset_dir}/_BEGINRUNNING.${cdate}.${chour}
		$spark_submit \
		--class $run_offset_class \
		--name $run_offset_job_name \
		--keytab $keytab \
		--principal $principal \
		--master yarn \
		--deploy-mode cluster \
		--queue $run_offset_queue \
  	${run_offset_jar} $kafka_topic $brokers $kafka_offset_dir/$cdate/$chour $cdate_hour
  	hdfs dfs -rm -r ${kafka_offset_dir}/_BEGINRUNNING.${cdate}.${chour}
fi

if [ $? -ne 0 ]
then
  sendmessage "wechat,mail" "error" "yzb collect $kafka_topic getoffset test error($cdate:$chour)" "yzb collect $kafka_topic getoffset test job:${run_offset_job_name} failed!" zhangxiangwei
  echo "GetOffsetError ---- offsetDir: ${kafka_offset_dir}/${cdate}/$chour/_SUCCESS, Please Check!"
  hdfs dfs -rm -r ${kafka_offset_dir}/_BEGINRUNNING.${cdate}.${chour}
  exit 1
fi

# 3.3 collect
if $(hadoop fs -test -e ${hdfs_source_dir}/${cdate}/$chour/_SUCCESS)
then
  if [ $must_run -eq 1 ]
  then
  	hdfs dfs -rm -r $hdfs_source_dir/$cdate/$chour
  	echo "HdfsFile ${hdfs_source_dir}/${cdate}/$chour/_SUCCESS exists, but must_run=${must_run}, so must rerun collect, Please wait......"
		$spark_submit \
		--class $run_collect_class \
		--name $run_collect_job_name \
		--keytab $keytab  \
		--principal $principal  \
		--master yarn \
		--deploy-mode cluster \
		--executor-memory 4G \
		--num-executors 4 \
		--queue $run_collect_queue \
		$run_collect_jar ${kafka_offset_dir}/${cdate}/${chour} $brokers $kafka_topic $hdfs_source_dir/$cdate/$chour
	else
		echo "HdfsFile ${hdfs_source_dir}/${cdate}/$chour/_SUCCESS exists, not to rerun collect"
	fi
else
	hdfs dfs -rm -r $hdfs_source_dir/$cdate/$chour
	echo "HdfsFile ${hdfs_source_dir}/${cdate}/$chour/_SUCCESS not exists, must to run collect, Please wait......"
	$spark_submit \
	--class $run_collect_class \
	--name $run_collect_job_name \
	--keytab $keytab  \
	--principal $principal  \
	--master yarn \
	--deploy-mode cluster \
	--executor-memory 4G \
	--num-executors 4 \
	--queue $run_collect_queue \
	$run_collect_jar ${kafka_offset_dir}/${cdate}/${chour} $brokers $kafka_topic $hdfs_source_dir/$cdate/$chour
fi

if [ $? -ne 0 ]
then
  sendmessage "wechat,mail" "error" "yzb collect $kafka_topic kafka2hdfs test error(${cdate}:${chour})" "yzb collect $kafka_topic kafka2hdfs test job:${run_collect_job_name} failed!" zhangxiangwei
  echo "yzb collect yzb_api kafka2hdfs error(${cdate}:${chour}), Please Check!"
  exit 1
fi

# 3.4 rm expired offset
if $(hadoop fs -test -d ${kafka_offset_dir}/${del_data_expire})
then
	hadoop fs -rmr $kafka_offset_dir/${del_data_expire}
fi
echo "finished ($cdate:$chour)"
