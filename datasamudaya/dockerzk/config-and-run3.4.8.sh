#!/bin/bash

echo "$SERVER_ID / $MAX_SERVERS"

if [ ! -z "$SERVER_ID" ] && [ ! -z "$MAX_SERVERS" ]; then
  echo "Starting up in clustered mode"
  echo "" >> /opt/zookeeper/zookeeper-3.4.8/conf/zoo.cfg
  echo "#Server List" >> /opt/zookeeper/zookeeper-3.4.8/conf/zoo.cfg
  transport=2888
  leaderport=3888
  clientport=2181
  clientport=$(( $clientport + $SERVER_ID ))
  echo "clientPort=$clientport" >> /opt/zookeeper/zookeeper-3.4.8/conf/zoo.cfg
  for i in $( eval echo {1..$MAX_SERVERS});do
  	transport=$(( $transport + 2000 ))
  	leaderport=$(( $leaderport + 2000 ))  	
    if [ "$SERVER_ID" = "$i" ];then
      echo "server.$i=0.0.0.0:$transport:$leaderport" >> /opt/zookeeper/zookeeper-3.4.8/conf/zoo.cfg
    else
      echo "server.$i=zookeeper-$i:$transport:$leaderport" >> /opt/zookeeper/zookeeper-3.4.8/conf/zoo.cfg
    fi
  done
  cat /opt/zookeeper/zookeeper-3.4.8/conf/zoo.cfg

  # Persists the ID of the current instance of Zookeeper
  echo $SERVER_ID > /opt/zookeeper/zookeeper-3.4.8/data/myid
  else
          echo "Starting up in standalone mode"
fi
cd /opt/zookeeper/zookeeper-3.4.8
exec /opt/zookeeper/zookeeper-3.4.8/bin/zkServer.sh start-foreground