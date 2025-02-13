#!/bin/bash
echo STARTING SQLClient....
export DEBUGPORT=4005
if [[ ! -v DATASAMUDAYA_HOME ]]; then
	echo "DATASAMUDAYA_HOME is not configured, configuring..."	
	export DATASAMUDAYA_HOME=$(pwd)/..
else
	echo "DATASAMUDAYA_HOME is set as $DATASAMUDAYA_HOME"
fi
export CLASSPATH="-cp .:$DATASAMUDAYA_HOME/ailib/*:$DATASAMUDAYA_HOME/jars/*:$DATASAMUDAYA_HOME/yarnlib/*"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
export CLASSNAME=com.github.datasamudaya.stream.sql.SQLClient
export MEMCONFIGLOW=-Xms512m
export MEMCONFIGHIGH=-Xmx512m
export GCCCONFIG=-XX:+UseZGC
java $CLASSPATH $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME -user arun -containerssql 1 -containercpu 2 -containermemory 2048 -drivercpu 1 -drivermemory 2048 -isdriverrequired false -sqlworkermode standalone
