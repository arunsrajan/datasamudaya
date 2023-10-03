@echo off

title DATASAMUDAYA MR SQL

setLocal EnableDelayedExpansion

echo STARTING SQL....

IF "%DATASAMUDAYA_HOME%" == "" (
@echo on
set DATASAMUDAYA_HOME=%~dp0/..
echo DATASAMUDAYA home is not configured, configuring DATASAMUDAYA_HOME...
@echo off
) ELSE (
@echo on
echo DATASAMUDAYA home is configured as "%DATASAMUDAYA_HOME%"
@echo off
)
set DEBUGPORT=3001

set ZOOKEEPERADMINCONFIG=-Dzookeeper.admin.serverPort=2180

set CLASSPATH=-classpath ".;../lib/*;../modules/*"

set DEBUGCONFIG=-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=%DEBUGPORT%,suspend=n

set MEMCONFIG=-Xms128m -Xmx256m

set ADDOPENSMODULES=--enable-preview --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-modules jdk.incubator.foreign --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.sql/java.sql=ALL-UNNAMED

set GCCONFIG=-XX:+UseZGC -XX:InitiatingHeapOccupancyPercent=80

IF EXIST %DATASAMUDAYA_JAVA_HOME%\bin\java.exe (

"%DATASAMUDAYA_JAVA_HOME%\bin\java" -version

"%DATASAMUDAYA_JAVA_HOME%\bin\java" %MEMCONFIG% %ADDOPENSMODULES% %GCCONFIG% %DEBUGCONFIG% %CLASSPATH% -Djava.net.preferIPv4Stack=true com.github.datasamudaya.tasks.scheduler.sql.SQLClientMR -user default

) ELSE (
 @echo on
 echo %DATASAMUDAYA_JAVA_HOME% doesnot exists, please set JAVA_HOME environment variable with correct path.
 pause
)

