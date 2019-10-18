#!/bin/bash
hdfs dfs -mkdir -p /tmp/synthetic_1m_2/uniform_1
hdfs dfs -mkdir -p /tmp/synthetic_1m_2/uniform_2
hdfs dfs -mkdir -p /tmp/synthetic_1m_2/normal_1
hdfs dfs -mkdir -p /tmp/synthetic_1m_2/normal_2
hdfs dfs -mkdir -p /tmp/synthetic_1m_2/powerlaw_1
hdfs dfs -mkdir -p /tmp/synthetic_1m_2/powerlaw_2

hdfs dfs -put ./data/t_1000000n_10000k_uniform_1.csv       /tmp/synthetic_1m_2/uniform_1
hdfs dfs -put ./data/t_1000000n_10000k_uniform_2.csv       /tmp/synthetic_1m_2/uniform_2
hdfs dfs -put ./data/t_1000000n_10000k_normal_1.csv       /tmp/synthetic_1m_2/normal_1
hdfs dfs -put ./data/t_1000000n_10000k_normal_2.csv       /tmp/synthetic_1m_2/normal_2
hdfs dfs -put ./data/t_1000000n_10000k_powerlaw_1.csv       /tmp/synthetic_1m_2/powerlaw_1
hdfs dfs -put ./data/t_1000000n_10000k_powerlaw_2.csv       /tmp/synthetic_1m_2/powerlaw_2
