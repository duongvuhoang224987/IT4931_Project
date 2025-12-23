#!/bin/bash
# rm -rf /opt/hadoop/data/datanode/*
chown -R hadoop:hadoop /opt/hadoop/data/datanode
chmod 755 /opt/hadoop/data/datanode
hdfs datanode