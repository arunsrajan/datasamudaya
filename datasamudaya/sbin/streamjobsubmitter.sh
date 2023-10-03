#!/usr/bin/bash
echo STARTING Stream Job Submitter....
export DEBUGPORT=4004
if [[ ! -v DATASAMUDAYA_HOME ]]; then
	echo "DATASAMUDAYA_HOME is not configured, configuring..."	
	export DATASAMUDAYA_HOME=$(pwd)/..
else
	echo "DATASAMUDAYA_HOME is set as $DATASAMUDAYA_HOME"
fi
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
export IPV4="-Djava.net.preferIPv4Stack=true"
export CLASSNAME=com.github.datasamudaya.stream.submitter.StreamPipelineJobSubmitter
export CLASSPATH="-cp .:../lib/*:../modules/*"
export MEMCONFIGLOW=-Xms512m
export MEMCONFIGHIGH=-Xmx512m
export GCCCONFIG=-XX:+UseZGC
export ADDOPENSMODULES="--add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-modules jdk.incubator.foreign --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.sql/java.sql=ALL-UNNAMED"
java $ADDOPENSMODULES $CLASSPATH $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG $IPV4 -Djava.net.preferIPv4Stack=true $CLASSNAME $@
