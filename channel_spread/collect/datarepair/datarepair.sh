#!/bin/bash

more kafka.log | awk -F "\001" '{print $5}' | awk -F 'talkingdata.' '{print $2}' | awk -F ' HTTP' '{print $1}' > kafka_20170612.log 

#gzip file
#gunzip file.gz
