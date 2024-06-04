#!/usr/bin/bash
echo STARTING Standalone Schedulers....
export DEBUGPORT=4000
if [[ ! -v DATASAMUDAYA_HOME ]]; then
	echo "DATASAMUDAYA_HOME is not configured, configuring..."	
	export DATASAMUDAYA_HOME=$(pwd)/..
else
	echo "DATASAMUDAYA_HOME is set as $DATASAMUDAYA_HOME"
fi
export CLASSPATH="-cp .:../lib/*:../modules/*"
export DEBUGCONFIG="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$DEBUGPORT,suspend=n"
export CLASSNAME=com.github.datasamudaya.tasks.scheduler.executor.standalone.EmbeddedSchedulersNodeLauncher
export MEMCONFIGLOW=-Xms1g 
export MEMCONFIGHIGH=-Xmx10g 
export GCCCONFIG=-XX:+UseZGC
export ADDOPENSMODULES="--add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.sql/java.sql=ALL-UNNAMED"
echo java $ADDOPENSMODULES $CLASSPATH $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME "$@"
java $ADDOPENSMODULES $CLASSPATH -javaagent:../lib/dsagent-3.0.jar $MEMCONFIGLOW $MEMCONFIGHIGH $GCCCONFIG -Djava.net.preferIPv4Stack=true $CLASSNAME $@