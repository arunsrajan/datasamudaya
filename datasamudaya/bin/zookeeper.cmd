title Zookeeper

@echo off

setLocal EnableDelayedExpansion

echo STARTING Zookeeper....

IF "%DATASAMUDAYA_HOME%" == "" (
@echo on
set DATASAMUDAYA_HOME=%~dp0..
echo DATASAMUDAYA home is not configured, configuring DATASAMUDAYA_HOME...
@echo off
) ELSE (
@echo on
echo DATASAMUDAYA home is configured as "%DATASAMUDAYA_HOME%"
@echo off
)

set DEBUGPORT=4200

set CLASSPATH=-classpath ".;../lib/*;../modules/*"

set DEBUGCONFIG=-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=%DEBUGPORT%,suspend=n

set MEMCONFIG=-Xms1g -Xmx1g

set GCCONFIG=-XX:+UseZGC -XX:InitiatingHeapOccupancyPercent=80

IF EXIST %DATASAMUDAYA_JAVA_HOME%\bin\java.exe (

"%DATASAMUDAYA_JAVA_HOME%\bin\java" -version

"%DATASAMUDAYA_JAVA_HOME%\bin\java" %MEMCONFIG% %GCCONFIG% %DEBUGCONFIG% %CLASSPATH% -Djava.net.preferIPv4Stack=true com.github.datasamudaya.common.utils.ZookeeperMain

) ELSE (
 @echo on
 echo %DATASAMUDAYA_JAVA_HOME% doesnot exists, please set JAVA_HOME environment variable with correct path.
 pause
)

