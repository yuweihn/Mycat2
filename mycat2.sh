#!/bin/bash

jdk_path=${1}
prj_name='mycat2'
if  [ ! -n "${jdk_path}" ] ;then
    jdk_path='java'
fi

ID=`ps -ef|grep ${prj_name}.jar | grep java | grep -v grep | awk '{print $2}'`
for id in $ID
do
  kill -9 $id
  echo "killed $id"
done
sleep 1

${jdk_path} -Xms1024m -Xmx1024m -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=256m -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=45 -XX:MaxGCPauseMillis=200 -Xloggc:gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintHeapAtGC -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=100m -jar ${prj_name}.jar prd >/dev/null 2>&1&
sleep 1

echo "ok......"
