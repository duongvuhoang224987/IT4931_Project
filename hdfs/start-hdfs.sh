#!/bin/bash
if [ ! -d "/opt/hadoop/data/namenode/current" ]; then
    echo "Formatting NameNode..."
    hdfs namenode -format
fi
hdfs namenode