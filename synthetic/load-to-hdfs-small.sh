#!/bin/bash
hdfs dfs -mkdir -p /tmp/synthetic_small/uniform_1
# hdfs dfs -mkdir -p /tmp/synthetic_small/uniform_uniform_1
# hdfs dfs -mkdir -p /tmp/synthetic_small/uniform_normal_1
# hdfs dfs -mkdir -p /tmp/synthetic_small/uniform_powerlaw_1
# hdfs dfs -mkdir -p /tmp/synthetic_small/normal_powerlaw_1
hdfs dfs -mkdir -p /tmp/synthetic_small/uniform_2
# hdfs dfs -mkdir -p /tmp/synthetic_small/normal_1
# hdfs dfs -mkdir -p /tmp/synthetic_small/normal_2
# hdfs dfs -mkdir -p /tmp/synthetic_small/powerlaw_1
# hdfs dfs -mkdir -p /tmp/synthetic_small/powerlaw_2
# hdfs dfs -mkdir -p /tmp/synthetic_small/uniform_max_var_2
# hdfs dfs -mkdir -p /tmp/synthetic_small/normal_max_var_2
# hdfs dfs -mkdir -p /tmp/synthetic_small/powerlaw_max_var_2
hdfs dfs -put ./data/t_10000000n_1000000k_uniform_1.csv       /tmp/synthetic_small/uniform_1
# hdfs dfs -put ./data/t_10000000n_1000000k_uniform_uniform_1.csv       /tmp/synthetic_small/uniform_uniform_1
# hdfs dfs -put ./data/t_10000000n_1000000k_uniform_normal_1.csv       /tmp/synthetic_small/uniform_normal_1
# hdfs dfs -put ./data/t_10000000n_1000000k_uniform_powerlaw_1.csv       /tmp/synthetic_small/uniform_powerlaw_1
# hdfs dfs -put ./data/t_1000000n_100000k_normal_powerlaw_1.csv       /tmp/synthetic_small/normal_powerlaw_1
hdfs dfs -put ./data/t_10000000n_1000000k_uniform_2.csv       /tmp/synthetic_small/uniform_2
# hdfs dfs -put ./data/t_10000000n_1000000k_normal_1.csv       /tmp/synthetic_small/normal_1
# hdfs dfs -put ./data/t_1000000n_100000k_normal_2.csv       /tmp/synthetic_small/normal_2
# hdfs dfs -put ./data/t_10000000n_1000000k_powerlaw_1.csv       /tmp/synthetic_small/powerlaw_1
# hdfs dfs -put ./data/t_10000000n_1000000k_powerlaw_2.csv       /tmp/synthetic_small/powerlaw_2
# hdfs dfs -put ./data/t_10000000n_1000000k_uniform_max_var_2.csv       /tmp/synthetic_small/uniform_max_var_2
# hdfs dfs -put ./data/t_10000000n_1000000k_normal_max_var_2.csv       /tmp/synthetic_small/normal_max_var_2
# hdfs dfs -put ./data/t_10000000n_1000000k_powerlaw_max_var_2.csv       /tmp/synthetic_small/powerlaw_max_var_2
