#!/bin/bash
hdfs dfs -mkdir -p /tmp/synthetic_10m/normal_powerlaw2_1
hdfs dfs -put ./data/t_100000000n_10000000k_normal_powerlaw2_1.csv       /tmp/synthetic_10m/normal_powerlaw2_1