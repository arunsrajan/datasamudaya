#!/bin/sh
echo $CORE_CONF_fs_defaultFS
echo $HDFS_CONF_dfs_datanode_data_dir
cd "/opt/datasamudaya/sbin"
./standalone.sh &
cd "/"
./entrypoint.sh
./run.sh
