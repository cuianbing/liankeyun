#!/bin/bash
hosts=(lky01 lky02 lky03)
for data in ${hosts[@]};
do
    ssh root@${data} "zkServer.sh start"
done

ssh root@${hosts[0]} "start-dfs.sh"
ssh root@${hosts[1]} "start-yarn.sh"
ssh root@${hosts[2]} "yarn-daemon.sh start resourcemanager"
ssh root@${hosts[0]} "start-spark-all.sh"

ssh root@${hosts[0]} "/opt/sqoop/bin sqoop import --connect jdbc:mysql://${1}:3306/retail  --username root 
--password 123456 --table consume_notes --hive-import --create-hive-table -m 1
"
ssh root@${hosts[0]} "/opt/sqoop/bin sqoop import --connect jdbc:mysql://${1}:3306/retail  --username root 
--password 123456 --table hy_transform --hive-import --create-hive-table -m 1
"
# ssh root@${hosts[0]} "/opt/hive/hive --hiveconf hive.cli.print.current.db=true"