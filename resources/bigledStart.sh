#!/bin/bash
#su hdfs
workdir=/opt/soft/bigLED
chown hdfs:hdfs ${workdir}/bigLED-1.0-SNAPSHOT-shaded.jar
chown hdfs:hdfs ${workdir}/bigled.log
pid=`ps -ef | grep  java | grep bigLED-1.0-SNAPSHOT-shaded.jar | grep -v grep |awk '{print $2}'`
kill -9 ${pid}
hdfs dfs -rm -r /tmp/bigled/refrash.parquet #升级时删除，自拉启监控时不删除
/opt/cloudera/parcels/CDH/bin/hdfs dfs -rm -r /user/hdfs/BigLEDCheckPoint && sleep 3
nohup spark-submit --class com.tc.BigLED --master yarn-client --conf spark.eventLog.enabled=false --num-executors 2 --executor-memory 1g --executor-cores 2 ${workdir}/bigLED-1.0-SNAPSHOT-shaded.jar > ${workdir}/bigled.log 2>&1 &
#exit
