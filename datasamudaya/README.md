The datasamudaya can be build using the following maven goals

mvn -Dmaven.antrun.skip=true -Dmaven.test.skip=true -Pmodules clean rewrite:run

mvn -Dmaven.antrun.skip=true -Dmaven.test.skip.exec=true -DskipMavenParsing=true -Pmodules clean install assembly:assembly

To Build Only Requires Containers
---------------------------------
mvn -f pomjar.xml -Pdatasamudaya exec:exec antrun:run@prepare compile jib:dockerBuild@buildstandalone jib:dockerBuild@buildcontainer jib:dockerBuild@buildzookeeper

To Build All Containers
-----------------------
mvn -f pomjar.xml -Pdatasamudaya exec:exec antrun:run@prepare compile jib:dockerBuild@buildstandalone jib:dockerBuild@buildcontainer jib:dockerBuild@buildzookeeper jib:dockerBuild@buildtaskschedulerstream jib:dockerBuild@buildtaskscheduler

In order to skip tests the following needs to be set in MAVEN_OPTS
------------------------------------------------------------------
-Dmaven.test.skip.exec=true

and mvn command with goals are 

mvn -Dmaven.antrun.skip=true -Dmaven.test.skip.exec=true -Pmodules clean package assembly:assembly

To Run jacoco and sonar
-----------------------

-Dmaven.antrun.skip=true -f pom.xml -Pmodules jacoco:report-aggregate sonar:sonar -Dsonar.projectKey=datasamudaya -Dsonar.projectName='datasamudaya' -Dsonar.host.url=http://localhost:9900 -Dsonar.token=sqp_821cb5fc68adfa589c838348c5fe1ab9049414d4 -Dsonar.sources=pom.xml,src/main -Dsonar.test.exclusions=**src/test/**/*.java -Dsonar.language=Java

To compile only tests
---------------------------------------------------------------
mvn -Dmaven.antrun.skip=true -Pmodules clean compile test-compile

To run tests
------------
mvn -Dmaven.antrun.skip=true  -f pom.xml -Pmodules clean test

To run specific test cases to specific module
----------------------------------------------------------
mvn -Dmaven.antrun.skip=true -DfailIfNoTests=false -Dtest="StreamPipelineTestSuite"  -f pom.xml -Pmodules clean test

In order to build docker images please execute the following maven goals
------------------------------------------------------------------------
-Pdatasamudayacontainer docker:build

-Pdatasamudayataskschedulerstream docker:build

-Pdatasamudayataskscheduler docker:build



In order to build docker images using docker commands
-----------------------------------------------------
docker build -t arunsrajan/datasamudayacontainer .
docker push arunsrajan/datasamudayacontainer
docker build -t arunsrajan/datasamudayataskschedulerstream .
docker push arunsrajan/datasamudayataskschedulerstream
docker build -t arunsrajan/datasamudayataskscheduler .
docker push arunsrajan/datasamudayataskscheduler
docker build -t arunsrajan/datasamudayastandalone .
docker push arunsrajan/datasamudayastandalone


To execute all tests only Stream Modulese
---------------------------------------
clean install -Dtest=Massive*,Pipeline* -pl MassiveDataCommon,MassiveDataStream test -DfailIfNoTests=false


To run docker container using bridge network use the following command
-----------------------------------------------------------------------

To run as 3 node zookeeper ensemble in separate bridge network
--------------------------------------------------------------
docker network create zoocluster -d bridge

docker run -d -p 2181:2181 -e ZOO_MY_ID=1 -e ZOO_SERVERS="server.1=0.0.0.0:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=zoo3:2888:3888;2181" --network=zoocluster --hostname=zoo1 -d --name=zookeeper1 zookeeper

docker run -d -p 2182:2181 -e ZOO_MY_ID=2 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=0.0.0.0:2888:3888;2181 server.3=zoo3:2888:3888;2181" --network=zoocluster --hostname=zoo2 -d --name=zookeeper2 zookeeper

docker run -d -p 2183:2181 -e ZOO_MY_ID=3 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=0.0.0.0:2888:3888;2181" --network=zoocluster --hostname=zoo3 -d --name=zookeeper3 zookeeper

To run zookeeper as single node in network datasamudaya
----------------------------------------------
docker run --network datasamudaya --name zoo -p 2181:2181 --hostname zoo -d arunsrajan/datasamudayazk


To run datasamudaya as separate node launcher in network datasamudaya
----------------------------------------------
docker network create --driver=bridge --subnet=172.30.0.0/16 --ip-range=172.30.0.0/16 datasamudaya --attachable

docker run --network datasamudaya --name namenode --hostname namenode -v "E:/DEVELOPMENT/dockershare:/opt/dockershare" -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "HDFS_CONF_dfs_namenode_name_dir=file:///opt/dockershare" -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" -e "CLUSTER_NAME=hadooptest" -p 9870:9870 -p 9000:9000 -d bde2020/hadoop-namenode

docker run --network datasamudaya -v "C:/DEVELOPMENT/dockershare/container:/opt/dockershare" --hostname dnte --link namenode:namenode --link zoo:zoo -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" -e "HDFS_CONF_dfs_datanode_data_dir=/opt/dockershare/data" --name datasamudayacontainer --ip 172.30.0.20 -e ZKHOSTPORT=zoo:2181 -e HOST=172.30.0.20 -e PORT=10101 -e NODEPORT=12121 -p 12121:12121 --memory 4g -e MEMCONFIGLOW=-Xms512M -e MEMCONFIGHIGH=-Xmx512M -d arunsrajan/datasamudayacontainer

To run task scheduler stream in network datasamudaya
-------------------------------------------

docker run --network datasamudaya -v "C:/DEVELOPMENT/dockershare:/opt/dockershare" --link namenode:namenode --link zoo:zoo --hostname datasamudayatss --name datasamudayataskschedulerstream --ip 172.30.0.22 -e ZKHOSTPORT=zoo:2181 -e HOST=172.30.0.22 -e PORT=22222 -p 22222:22222 -p 22223:22223 -e DEBUGPORT=*:4005 -p 4005:4005 --memory 3g  -e MEMCONFIGLOW=-Xms2G -e MEMCONFIGHIGH=-Xmx2G -d arunsrajan/datasamudayataskschedulerstream

To run task scheduler in network datasamudaya
-------------------------------------------

docker run --network datasamudaya -v "C:/DEVELOPMENT/dockershare:/opt/dockershare" --name datasamudayataskscheduler --link namenode:namenode --link zoo:zoo --hostname datasamudayats --ip 172.30.0.23 -e ZKHOSTPORT=zoo:2181 -e HOST=172.30.0.23 -e PORT=11111 -p 11111:11111 -p 11112:11112 -e DPORT=*:4000 -p 4000:4000 --memory 3g -e MEMCONFIGLOW=-Xms2G -e MEMCONFIGHIGH=-Xmx2G -d arunsrajan/datasamudayataskscheduler

To run standalone in network datasamudaya
------------------------------------------- 
docker run --network datasamudaya --name namenode --hostname namenode -v "C:/DEVELOPMENT/dockershare:/opt/dockershare" -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "HDFS_CONF_dfs_namenode_name_dir=file:///opt/dockershare/name" -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" -e "CLUSTER_NAME=hadooptest" -p 9870:9870 -p 9000:9000 -d bde2020/hadoop-namenode

docker run --network datasamudaya -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" -v "C:/DEVELOPMENT/dockershare:/opt/dockershare" -e "HDFS_CONF_dfs_datanode_data_dir=/opt/dockershare/data" --name datasamudayastandalone --hostname datasamudayastandalone --ip 172.30.0.10 -e ZKHOSTPORT=zoo:2181 -e TEHOST=172.30.0.10 -e TEPORT=10101 -e NODEPORT=12121 -e TSSHOST=172.30.0.10 --link namenode:namenode -e TSSPORT=22222 -e TSHOST=172.30.0.10 -e TSPORT=11111 -p 22222:22222 -p 22223:22223 -p 11111:11111 -p 11112:11112 -p 12123:12123 -p 12124:12124 -e DPORT=*:4000 -p 4000:4000 --memory 4g -e MEMCONFIGLOW=-Xms512m -e MEMCONFIGHIGH=-Xmx512m -d arunsrajan/datasamudayastandalone

To run docker container as separate service in swarm using weave networks to support multicasting
----------------------------------------------------
docker plugin install weaveworks/net-plugin:latest_release
docker plugin disable weaveworks/net-plugin:latest_release
docker plugin set weaveworks/net-plugin:latest_release WEAVE_PASSWORD=
docker plugin set weaveworks/net-plugin:latest_release WEAVE_MULTICAST=1
docker plugin enable weaveworks/net-plugin:latest_release

sudo rm /run/docker/plugins/weave.sock
sudo rm /run/docker/plugins/weavemesh.sock

sudo curl -L git.io/weave -o /usr/local/bin/weave
sudo chmod a+x /usr/local/bin/weave

./weave connect 192.168.99.104

./weave connect 192.168.99.111

./weave connect 192.168.49.4

docker swarm init --advertise-addr 192.168.99.104 

docker swarm join --token SWMTKN-1-5g3bdtqdlqkgxjly8d66dbl2u28pd5cxt1haueo1eu0tc8a9j3-8zzmiks0lm6bar1n9r7y5pkfz 192.168.99.104:2377

docker network create --driver weaveworks/net-plugin:latest_release weave --attachable

docker service create --name zooweave --endpoint-mode dnsrr --network weave -d mesoscloud/zookeeper:3.4.8-centos-7

docker service update --publish-add published=2181,target=2181,protocol=tcp,mode=host zooweave

docker service create --name tesweave --endpoint-mode dnsrr --network weave -e HOST="{{.Service.Name}}.{{.Task.Slot}}.{{.Task.ID}}" -e ZKHOSTPORT=zooweave:2181 -e PORT=10101 -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx4G --replicas=3 arunsrajan/taskexecutorstream

docker service create --name tssweave --endpoint-mode vip -e MEMCONFIGLOW=-Xms1024M -e MEMCONFIGHIGH=-Xmx4096M --network weave -p 32325:22222 -e HOST="{{.Service.Name}}.{{.Task.Slot}}.{{.Task.ID}}" -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zooweave:2181 -e PORT=22222 arunsrajan/taskschedulerstream

docker service create --name tsweave --endpoint-mode vip --network weave -p 32326:11111 -e HOST="{{.Service.Name}}.{{.Task.Slot}}.{{.Task.ID}}" -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zooweave:2181 -e PORT=11111 arunsrajan/taskscheduler

docker service create --network weave --mount source=/dataset,target=/mnt/sftp/dataset,type=bind --name namenode --endpoint-mode dnsrr sequenceiq/hadoop-docker /etc/bootstrap.sh -bash

docker run --rm -e DAEMONS=namenode,datanode,secondarynamenode -v //d/sftp:/mnt/sftp/dataset --network weave --name=namenode -p 50070:50070 -p 50075:50075 -p 50090:50090 -p 9000:9000 cybermaggedon/hadoop:2.10.0 /start-namenode

docker run --rm -e DAEMONS=datanode --network weave --name=datanode --link namenode:namenode -e NAMENODE_URI=hdfs://namenode:9000 cybermaggedon/hadoop:2.10.0 /start-datanode


cd /opt/hadoop-3.2.1/bin

./hadoop dfs -mkdir /airline1989

./hadoop dfs -mkdir /carriers

./hadoop dfs -put /opt/dockershare/1989.csv /airline1989

./hadoop dfs -put /opt/dockershare/carriers.csv /carriers

./hadoop dfs -mkdir /mds

./hadoop dfs -chmod 777 /mds

./hadoop dfs -mkdir /newmapperout

./hadoop dfs -chmod 777 /newmapperout


To remove dangling images
--------------------------

docker rmi -f $(docker images -f "dangling=true" -q)


To install pods in kubernetes and upload data.
----------------------------------------------


docker tag arunsrajan/datasamudayataskschedulerstream localhost:5000/datasamudayataskschedulerstream
docker tag arunsrajan/datasamudayataskscheduler localhost:5000/datasamudayataskscheduler
docker tag arunsrajan/datasamudayacontainer localhost:5000/datasamudayacontainer


docker push localhost:5000/datasamudayataskschedulerstream
docker push localhost:5000/datasamudayataskscheduler
docker push localhost:5000/datasamudayacontainer


./hadoop dfs -mkdir /airlines

./hadoop dfs -chmod 777 /airlines

./hadoop dfs -mkdir /carriers

./hadoop dfs -chmod 777 /carriers

./hadoop dfs -put /mnt/sftp/dataset/dataset/1987.csv /airlines

./hadoop dfs -put /mnt/sftp/dataset/dataset/carriers.csv /carriers

./hadoop dfs -mkdir /mds

./hadoop dfs -chmod 777 /mds

./hadoop dfs -mkdir /newmapperout

./hadoop dfs -chmod 777 /newmapperout


To install weave scope
----------------------

sudo curl -L git.io/scope -o /usr/local/bin/scope
sudo chmod a+x /usr/local/bin/scope
scope launch

To run on kubernetes 
--------------------
minikube start -p datasamudaya --driver docker --mount=true --mount-string=C:\DEVELOPMENT\datasamudayakube:/minikube-host --cpus 4
echo "10.244.0.7      datasamudayastandalone-0" >> /etc/hosts
echo "10.244.0.8      datasamudayacontainer-0" >> /etc/hosts

minikube service datasamudayastandalone
kubectl port-forward --address "0.0.0.0" svc/namenode 9870:9870

To run the project in openshift
-------------------------------
oc adm policy add-scc-to-user hostaccess developer --as system:admin
oc adm policy add-cluster-role-to-user cluster-admin developer --as system:admin


oc create -f persistvolume.yaml
oc create -f pervolumeclaim.yaml
oc create -f datasamudayann.json
oc create -f datasamudayatss.json
oc create -f datasamudayazk.json
oc create -f pod.json

oc rsync D:/dataset/airlines namenode:/mnt/sftp/ -c hadoop-docker

cd /usr/local/hadoop/bin

./hadoop dfs -mkdir /airlines

./hadoop dfs -mkdir /carriers

./hadoop dfs -put /mnt/sftp/airlines/1987.csv /airlines

./hadoop dfs -put /mnt/sftp/airlines/1988.csv /airlines

./hadoop dfs -put /mnt/sftp/airlines/carriers.csv /carriers

./hadoop dfs -mkdir /mds

./hadoop dfs -chmod 777 /mds

./hadoop dfs -mkdir /newmapperout

./hadoop dfs -chmod 777 /newmapperout


To run the project in mesos
---------------------------

docker tag arunsrajan/mesos-master localhost:5000/mesos-master
docker tag arunsrajan/mesos-slave localhost:5000/mesos-slave

docker push localhost:5000/mesos-master
docker push localhost:5000/mesos-slave

docker network create --subnet=172.33.0.0/16 -d bridge datasamudayamesos

docker run --name zookeeper -p 2181:2181 -e ALLOW_ANONYMOUS_LOGIN=yes --network datasamudayamesos -d bitnami/zookeeper:3.5.7-debian-10-r23

docker run --name namenode --hostname namenode -v "D:/PROJECTS/hadoopspark:/opt/hadoopspark" --network=datasamudayamesos -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "CLUSTER_NAME=hadooptest" -p 9870:9870 -p 9000:9000 -d bde2020/hadoop-namenode

docker run --name datanode --hostname datanode --network=datasamudayamesos -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -d bde2020/hadoop-datanode

docker run --name mesos-master -p 5050:5050 -v "D:/PROJECTS/hadoopspark:/opt/hadoopspark" --ip 172.33.0.10 -p 22222:22222 --network datasamudayamesos -e MESOS_PORT=5050 -e MESOS_ZK=zk://zookeeper:2181/mesos -e MESOS_QUORUM=1 -e MESOS_REGISTRY=in_memory -e DEBUGPORT=*:4005 -p 4005:4005 -e MESOS_LOG_DIR=/var/log/mesos -e MESOS_WORK_DIR=/var/lib/mesos -e MESOS_HOSTNAME=172.33.0.10 -e MESOS_IP=172.33.0.10 -e MESOS_NATIVE_JAVA_LIBRARY=/usr/lib64/libmesos.so -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zookeeper:2181 -e PORT=22222 -e GCCCONFIG=-XX:+UseG1GC -e ISJGROUPS=false -e ISYARN=false -e ISMESOS=true -e MESOSMASTER=172.33.0.10:5050 -e HOST=172.33.0.10 -d arunsrajan/mesos-master

docker run --name mesos-slave --privileged -e GLOG_v=1 -v /var/run/docker.sock:/run/docker.sock -v "D:/PROJECTS/hadoopspark:/opt/hadoopspark" -p 5051:5051 --ip 172.33.0.11 --memory 10g --network datasamudayamesos -e MESOS_PORT=5051 -e MESOS_MASTER=zk://zookeeper:2181/mesos -e MESOS_QUORUM=1 -e MESOS_REGISTRY=in_memory -e MESOS_LOG_DIR=/var/log/mesos -e MESOS_LOGGING_LEVEL=INFO -e MESOS_WORK_DIR=/var/lib/mesos -e MESOS_NATIVE_JAVA_LIBRARY=/usr/lib64/libmesos.so -e MESOS_CONTAINERIZERS=mesos -d arunsrajan/mesos-slave

java -classpath ".:/opt/datasamudaya/lib/*:/opt/datasamudaya/modules/*" Deserialization
find /var -name "*stdout*" | grep Massive



To run the project in yarn
---------------------------

docker run --network datasamudaya -v /dataset:/mnt/sftp/dataset -e DAEMONS=namenode,datanode,secondarynamenode --name=namenode -p 50070:50070 -p 50075:50075 -p 50090:50090 -p 9000:9000 -d cybermaggedon/hadoop:2.10.0 /start-namenode

docker run --network datasamudaya -e DAEMONS=datanode -e NAMENODE_URI=hdfs://namenode:9000 --name=datanode --link namenode:namenode -P cybermaggedon/hadoop:2.10.0 /start-datanode 

docker run --network datasamudaya --ip 172.18.0.91 --name=resourcemanager -p 8088:8088 -d  cybermaggedon/hadoop:2.10.0 /start-resourcemanager

docker run --network datasamudaya --ip 172.18.0.92 -e RESOURCEMANAGER_HOSTNAME=resourcemanager --name=nodemanager --link resourcemanager:resourcemanager -d -P cybermaggedon/hadoop:2.10.0 /start-nodemanager


docker run --name yarntss -v /dataset:/mnt/sftp/dataset --ip 172.18.0.90 -p 22223:22222 --network datasamudaya -e MEMCONFIGLOW=-Xms1G -e HDFSNN=hdfs://namenode:9000 -e HDFSRM=resourcemanager:8032 -e HDFSRS=resourcemanager:8030 -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zookeeper:2181 -e PORT=22222 -e GCCCONFIG=-XX:+UseG1GC -e ISJGROUPS=false -e ISYARN=true -e ISMESOS=false -e HOST=172.18.0.90 -d arunsrajan/yarntss


Running Data Pipeline examples
------------------------------

To run examples download the dataset from the url

https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7

and create the /airline1989 folder in hadoop and upload the file after decompressing the file 1989.csv.bz2 to 1989.csv

Also download the carriers file (carriers.csv) from the above url create /carriers folder in hadoop and upload the file carriers.csv

Stream Reduce
------------
streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya


streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceMesos hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryUserAllocation hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya arun

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryImplicit hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1 2 512

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalJGroupsImplicit hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1 2 512

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDiskDAGCycleException hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemorySortByDelayResourceDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemorySortByAirlinesResourceDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya sa

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya local

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya yarn

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduce hdfs://127.0.0.1:9000 /github /examplesdatasamudaya jgroups

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.json.GithubEventsStreamReduceIgnite hdfs://127.0.0.1:9000 /github /examplesdatasamudaya

call streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.IgnitePipelineFile C:/DEVELOPMENT/dataset/airline/1987/

Stream Reduce LOJ
-----------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormalDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 64

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 128

Stream Reduce ROJ
-----------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya


streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroupsDivided  hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormalDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 128

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 128

Stream Reduce Transformations
-----------------------------
streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceSample hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalesceOne hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalescePartition hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceUnion hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceIntersection hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgnite hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoin hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReduction hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceSampleLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalesceOneLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalescePartitionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceUnionLocal hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceIntersectionLocal hdfs://127.0.0.1:9000 /airline1989 /1987 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgniteLocal hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReductionLocal hdfs://127.0.0.1:9000 /airline1989 /carriers /examplesdatasamudaya

Stream Reduce SQL
-----------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples hdfs://127.0.0.1:9000 /airline1989 /sqlexamplestandaloneoutput standalone true arun 2 4096

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples hdfs://127.0.0.1:9000 /airline1989 /sqlexamplejgroupsoutput jgroups true arun 2 4096

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.StreamingSqlExamples hdfs://127.0.0.1:9000 /airline1989 /sqlexampleyarnoutput yarn false

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumLocal hdfs://127.0.0.1:9000 /airline1989 /carriers 32 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers 16 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers 16 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSADisk hdfs://127.0.0.1:9000 /airline1989 /carriers 16 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlCountLocal hdfs://127.0.0.1:9000 /airline1989 16 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 64 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySADisk hdfs://127.0.0.1:9000 /airline1989 32 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemory hdfs://127.0.0.1:9000 /airline1989 32 1


streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 32 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelayLocal hdfs://127.0.0.1:9000 /airline1989 32 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySADisk hdfs://127.0.0.1:9000 /airline1989 32 1 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemory hdfs://127.0.0.1:9000 /1987 20432

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 32 1 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 128 11

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal hdfs://127.0.0.1:9000 /airline1989 128 11 /examplesdatasamudaya

Stream Reduce Aggregate
-----------------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayIgnite hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemory hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDiskDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroups hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroupsDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayYARN hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1


streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamCoalesceNormalInMemoryDiskContainerDivided hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1

Filter Operation Streaming
------------------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamFilterFilterCollectArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /examplesdatasamudaya 1024 1 32


Running MR Job examples
------------------------

mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /examplesdatasamudaya 128 10"


mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayYARN /airline1989 /carriers /examplesdatasamudaya 3 1024" 

mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayIGNITE /airline1989 /carriers /examplesdatasamudaya"


Running Job In Linux

./mapreducejobsubmitter.sh -jar ../examples/examples-2.0.jar -arguments  'com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /examplesdatasamudaya 128 3'

./streamjobsubmitter.sh ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroups  hdfs://namenode:9000 /airlines1989 /carriers /examplesdatasamudaya


Sonar
------
mvn org.sonarsource.scanner.maven:sonar-maven-plugin:3.4.0.905:sonar -Dsonar.login=38757a39a941623a4ef3b3cfb78bbe09181dfb5a -Dsonar.host.url=http://localhost:8082 -Dsonar.scm.disabled=true -Dsonar.language=Java,Scala

Stream Reduce Linux
-------------------
streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceIgnite  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroups  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroupsDivided  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemory hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDivided hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDisk hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryImplicit hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya 1 2 512

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalJGroupsImplicit hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya 1 2 512

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDiskDivided hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalInMemoryDiskDAGCycleException hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalDisk hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceNormalDiskDivided hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceYARN hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

Stream Reduce LOJ Limux
-----------------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinIgnite  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroups  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinJGroupsDivided  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormal hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinNormalDivided hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinYARN hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDisk hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 64

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceLeftOuterJoinInMemoryDiskDivided hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 128

Stream Reduce ROJ Linux
-----------------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinIgnite  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroups  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya


streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinJGroupsDivided  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormal hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinNormalDivided hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinYARN hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDisk hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 128

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceRightOuterJoinInMemoryDiskDivided hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya 1024 3 128

Stream Reduce Transformations Limux
-----------------------------------
streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceSample hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalesceOne hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCoalescePartition hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceUnion hdfs://namenode:9000 /airline1989 /1987 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceIntersection hdfs://namenode:9000 /airline1989 /1987 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReduceCachedIgnite hdfs://namenode:9000 /airline1989 /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoin hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoin hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoin hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairJoinCoalesceReduction hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairLeftJoinCoalesceReduction hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairRightJoinCoalesceReduction hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.transformation.examples.StreamReducePairMultipleJoinsCoalesceReduction hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya

Stream Reduce SQL Linux
------------------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumLocal hdfs://namenode:9000 /airline1989 /carriers 32 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemory hdfs://namenode:9000 /airline1989 /carriers 64 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSAInMemoryDisk hdfs://namenode:9000 /airline1989 /carriers 64 10

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlSumSADisk hdfs://namenode:9000 /airline1989 /carriers 96 10

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlCountLocal hdfs://namenode:9000 /airline1989 96 10

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelayLocal hdfs://namenode:9000 /airline1989 96 10

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySADisk hdfs://namenode:9000 /airline1989 32 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemory hdfs://namenode:9000 /airline1989 32 1


streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemoryDisk hdfs://namenode:9000 /airline1989 32 1


streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelayLocal hdfs://namenode:9000 /airline1989 32 2

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySADisk hdfs://namenode:9000 /airline1989 32 1 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemory hdfs://namenode:9000 /airline1989 32 1 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemoryDisk hdfs://namenode:9000 /airline1989 32 1 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelayLocal hdfs://namenode:9000 /airline1989 128 11

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.sql.examples.SqlUniqueCarrierYearMonthOfYearDayOfMonthSumCountArrDelaySaveLocal hdfs://namenode:9000 /airline1989 128 11 /examplesdatasamudaya

Stream Reduce Aggregate Linux
-----------------------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDisk hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayDiskDivided hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayIgnite hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemory hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDivided hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDisk hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayInMemoryDiskDivided hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDisk hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayCoalesceInMemoryDiskDivided hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroups hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayJGroupsDivided hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 3

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayLocal hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamAggSumCountArrDelayYARN hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamCoalesceNormalInMemoryDiskContainerDivided hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1

Filter Operation Streaming Linux
--------------------------------

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamFilterFilterCollectArrDelayInMemoryDisk hdfs://namenode:9000 /airline1989 /examplesdatasamudaya 1024 1 32


Running MR Job examples
------------------------

mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /examplesdatasamudaya 128 10"

mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayResourceDividedNormal /airline1989 /carriers /examplesdatasamudaya 128 10"

mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayYARN /airline1989 /carriers /examplesdatasamudaya 3 1024" 

mapreducejobsubmitter.cmd -jar ../examples/examples-2.0.jar -arguments "com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayIGNITE /airline1989 /carriers /examplesdatasamudaya"


Running Job In Linux

./mapreducejobsubmitter.sh -jar ../examples/examples-2.0.jar -arguments  'com.github.datasamudaya.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /examplesdatasamudaya 128 3'

./streamjobsubmitter.sh ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroups  hdfs://namenode:9000 /airlines1989 /carriers /examplesdatasamudaya


podman example for standalone
-----------------------------

podman network create --driver=bridge --subnet=172.30.0.0/16 --ip-range=172.30.0.0/16 datasamudaya

podman run --network datasamudaya --name namenode --hostname namenode -v "C:/DEVELOPMENT/dockershare:/opt/dockershare" -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "HDFS_CONF_dfs_namenode_name_dir=file:///opt/dockershare/name" -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" -e "CLUSTER_NAME=hadooptest" -p 9870:9870 -p 9000:9000 -d docker.io/bde2020/hadoop-namenode

podman run --network datasamudaya -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" -v "C:/DEVELOPMENT/dockershare:/opt/dockershare" --name datasamudayastandalone --hostname datasamudayastandalone --ip 172.30.0.10 -e ZKHOSTPORT=172.30.0.10:2181 -p 2181:2181 -e TEHOST=172.30.0.10 -e TEPORT=10101 -e NODEPORT=12121 -e TSSHOST=172.30.0.10 --link namenode:namenode -e TSSPORT=22222 -e TSHOST=172.30.0.10 -e TSPORT=11111 -p 22222:22222 -p 22223:22223 -p 11111:11111 -p 11112:11112 -e DPORT=*:4000 -p 4000:4000 --memory 4g -e MEMCONFIGLOW=-Xms512m -e MEMCONFIGHIGH=-Xmx512m -d arunsrajan/datasamudayastandalone


Modify distribution config datasamudaya.properties
-----------------------------------------

taskschedulerstream.hostport=127.0.0.1_22222

streamjobsubmitter.cmd ../examples/examples-2.0.jar com.github.datasamudaya.stream.examples.StreamReduceJGroupsDivided  hdfs://namenode:9000 /airline1989 /carriers /examplesdatasamudaya


datasamudayaSQL query
------------

create table airlines(AirlineYear varchar2(300),MonthOfYear varchar2(300),DayofMonth varchar2(300),DayOfWeek varchar2(300),DepTime varchar2(300),CRSDepTime varchar2(300),ArrTime varchar2(300),CRSArrTime varchar2(300),UniqueCarrier varchar2(300),FlightNum varchar2(300),TailNum varchar2(300),ActualElapsedTime varchar2(300),CRSElapsedTime varchar2(300),AirTime varchar2(300),ArrDelay varchar2(300),DepDelay varchar2(300),Origin varchar2(300),Dest varchar2(300),Distance varchar2(300),TaxiIn varchar2(300),TaxiOut varchar2(300),Cancelled varchar2(300),CancellationCode varchar2(300),Diverted varchar2(300),CarrierDelay varchar2(300),WeatherDelay varchar2(300),NASDelay varchar2(300),SecurityDelay varchar2(300),LateAircraftDelay varchar2(300), hdfslocation varchar2(300) default '/airlines')