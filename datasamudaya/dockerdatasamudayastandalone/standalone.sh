echo STARTING Standalone Schedulers....
export DEBUGPORT=4000
cd /opt/datasamudaya/sbin
if [[ ! -v DATASAMUDAYA_HOME ]]; then
	echo "DATASAMUDAYA_HOME is not configured, configuring..."	
	export DATASAMUDAYA_HOME=$(pwd)/..
else
	echo "DATASAMUDAYA_HOME is set as $DATASAMUDAYA_HOME"
fi
export ZOOADMINPORT=8040
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-classpath .:../lib/*:../modules/*"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
export CLASSNAME=com.github.datasamudaya.tasks.scheduler.executor.standalone.EmbeddedSchedulersNodeLauncher
export GCCCONFIG=-XX:+UseZGC
/usr/local/java/jdk-21.0.2/bin/java -javaagent:../modules/dsagent-3.0.jar $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskexecutor.host=$TEHOST" "-Dtaskexecutor.port=$TEPORT" "-Dnode.port=$NODEPORT" "-Dtaskschedulerstream.host=$TSSHOST" "-Dtaskschedulerstream.port=$TSSPORT" "-Dtaskscheduler.host=$TSHOST" "-Dtaskscheduler.port=$TSPORT" $ZOOKEEPERADMINCONFIG $DEBUGCONFIG $CLASSPATH -Djava.net.preferIPv4Stack=true $CLASSNAME
