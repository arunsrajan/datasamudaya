echo STARTING DataCruncher Task Executor....
export JMXPORT=33330
export DEBUGPORT=4000
cd /opt/datasamudaya/sbin
if [[ ! -v DATASAMUDAYA_HOME ]]; then
	echo "DATASAMUDAYA_HOME is not configured, configuring..."	
	export DATASAMUDAYA_HOME=$(pwd)/..
else
	echo "DATASAMUDAYA_HOME is set as $DATASAMUDAYA_HOME"
fi
export GCCCONFIG=-XX:+UseZGC
export ZOOKEEPERADMINCONFIG="-Dzookeeper.admin.serverPort=$ZOOADMINPORT"
export CLASSPATH="-cp '.:../lib/*:../modules/*'"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
/usr/local/java/jdk-21.0.2/bin/java "-Dhdfs.namenode.url=$NAMENODEURL" "-Dpodcidr.node.mapping.enabled=$PODCIDRNODEMAPPINGENABLED" "-Dzookeeper.hostport=$ZKHOSTPORT" "-Dtaskexecutor.host=$HOST" "-Dtaskexecutor.port=$PORT" "-Dnode.port=$NODEPORT" -classpath ".:../lib/*:../modules/*" $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true com.github.datasamudaya.tasks.executor.NodeLauncher
