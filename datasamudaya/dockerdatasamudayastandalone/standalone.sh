echo STARTING Standalone Schedulers....
export DEBUGPORT=4000
cd /opt/datasamudaya/bin
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
echo java --add-opens=java.base/java.nio=ALL-UNNAMED --enable-preview -XX:ActiveProcessorCount=4 --add-modules jdk.compiler -Xms512m -Xmx3g $ZOOKEEPERADMINCONFIG $DEBUGCONFIG $CLASSPATH -Djava.net.preferIPv4Stack=true $CLASSNAME
/usr/local/java/jdk-17.0.2/bin/java --enable-preview --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskexecutor.host=$TEHOST" "-Dtaskexecutor.port=$TEPORT" "-Dnode.port=$NODEPORT" "-Dtaskschedulerstream.host=$TSSHOST" "-Dtaskschedulerstream.port=$TSSPORT" "-Dtaskscheduler.host=$TSHOST" "-Dtaskscheduler.port=$TSPORT" --add-opens=java.base/java.nio=ALL-UNNAMED --enable-preview --add-modules jdk.compiler -Xms512m -Xmx3g $ZOOKEEPERADMINCONFIG $DEBUGCONFIG $CLASSPATH -Djava.net.preferIPv4Stack=true $CLASSNAME
