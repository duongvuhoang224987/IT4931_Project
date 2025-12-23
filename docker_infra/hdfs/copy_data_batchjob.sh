#!/bin/bash
DATA_DIR="$(dirname "$PWD")" 
docker exec nameNode hdfs dfsadmin -safemode leave
docker exec nameNode hdfs dfs -mkdir -p /user/it4931
docker cp $DATA_DIR/data/taxi_zone_lookup.csv nameNode:/data
docker cp $DATA_DIR/data/yellow_taxi/  nameNode:/data
docker exec nameNode hdfs dfs -put /data/yellow_taxi/ /user/it4931/
docker exec nameNode hdfs dfs -put /data/taxi_zone_lookup.csv /user/it4931/
