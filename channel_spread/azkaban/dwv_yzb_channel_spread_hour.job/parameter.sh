#!/bin/bash
if [ -z $1 ]||[ ${#1} -ne 8 ]||[ -z $2 ]||[ ${#2} -ne 2 ]
then
	must_run=0
	if [ $3 -eq 1 ]
	then
		must_run=1
	fi
	param='{"cdate":''"'$(date -d '1 hours ago' +%Y%m%d)'","chour":''"'$(date -d '1 hours ago' +%H)'"','"must_run":"'$must_run'"}'
	echo ${param}
	echo ${param} > $JOB_OUTPUT_PROP_FILE
else
	must_run=0
	if [ $3 -eq 1 ]
	then
		must_run=1
	fi
  param='{"cdate":''"'$1'","chour":''"'$2'"','"must_run":"'$must_run'"}'
  echo ${param}
  echo ${param} > $JOB_OUTPUT_PROP_FILE
fi
