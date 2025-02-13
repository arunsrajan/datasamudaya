#!/bin/bash
echo STARTING Standalone....
export DEBUGPORT=4002
if [[ ! -v DATASAMUDAYA_HOME ]]; then
	echo "DATASAMUDAYA_HOME is not configured, configuring..."	
	export DATASAMUDAYA_HOME=$(pwd)/..
else
	echo "DATASAMUDAYA_HOME is set as $DATASAMUDAYA_HOME"
fi
export CLASSPATH="-cp .:$DATASAMUDAYA_HOME/jars/*:$DATASAMUDAYA_HOME/yarnlib/*"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
export CLASSNAME=com.github.datasamudaya.tasks.scheduler.executor.standalone.EmbeddedSchedulersNodeLauncher
export MEMCONFIGLOW=-Xms512m
export MEMCONFIGHIGH=-Xmx512m
export GCCCONFIG=-XX:+UseZGC
java $CLASSPATH -javaagent:$DATASAMUDAYA_HOME/jars/dsagent-4.0.jar $MEMCONFIGLOW $DEBUGCONFIG $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME $@