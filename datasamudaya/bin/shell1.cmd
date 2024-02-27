@echo off

title Stream Api Scheduler

setLocal EnableDelayedExpansion

echo STARTING Stream Api Scheduler....

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

set DEBUGPORT=4000

set CLASSPATH=-classpath ".;../lib/*;../modules/*"

set DEBUGCONFIG=-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=%DEBUGPORT%,suspend=n

set MEMCONFIG=-Xms128M -Xmx256M

set ADDOPENSMODULES=--enable-preview --add-opens java.base/java.math=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.text=ALL-UNNAMED --add-opens java.sql/java.sql=ALL-UNNAMED

set GCCONFIG=-XX:+UseZGC -XX:InitiatingHeapOccupancyPercent=80

IF EXIST %DATASAMUDAYA_JAVA_HOME%\bin\java.exe (

"%DATASAMUDAYA_JAVA_HOME%\bin\java" -version

"%DATASAMUDAYA_JAVA_HOME%\bin\java" %MEMCONFIG% %ADDOPENSMODULES% %GCCONFIG% %CLASSPATH% -Djava.net.preferIPv4Stack=true com.github.datasamudaya.common.utils.JShellClient arun

) ELSE (
 @echo on
 echo %DATASAMUDAYA_JAVA_HOME% doesnot exists, please set JAVA_HOME environment variable with correct path.
 pause
)