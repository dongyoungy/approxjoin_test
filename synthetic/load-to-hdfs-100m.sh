#!/bin/bash
hdfs dfs -mkdir -p /tmp/synthetic_100m/1_uniform1_1
hdfs dfs -mkdir -p /tmp/synthetic_100m/1_uniform1_2
hdfs dfs -mkdir -p /tmp/synthetic_100m/2_uniform2_1
hdfs dfs -mkdir -p /tmp/synthetic_100m/2_uniform2_2
hdfs dfs -mkdir -p /tmp/synthetic_100m/3_normal1_1
hdfs dfs -mkdir -p /tmp/synthetic_100m/3_normal1_2
hdfs dfs -mkdir -p /tmp/synthetic_100m/4_normal2_1
hdfs dfs -mkdir -p /tmp/synthetic_100m/4_normal2_2
hdfs dfs -mkdir -p /tmp/synthetic_100m/5_powerlaw1_1
hdfs dfs -mkdir -p /tmp/synthetic_100m/5_powerlaw1_2
hdfs dfs -mkdir -p /tmp/synthetic_100m/6_powerlaw2_1
hdfs dfs -mkdir -p /tmp/synthetic_100m/6_powerlaw2_2

hdfs dfs -put ./data/t_100000000n_uniform1_1.csv       /tmp/synthetic_100m/1_uniform1_1
hdfs dfs -put ./data/t_100000000n_uniform1_2.csv       /tmp/synthetic_100m/1_uniform1_2
hdfs dfs -put ./data/t_100000000n_uniform2_1.csv       /tmp/synthetic_100m/2_uniform2_1
hdfs dfs -put ./data/t_100000000n_uniform2_2.csv       /tmp/synthetic_100m/2_uniform2_2
hdfs dfs -put ./data/t_100000000n_normal1_1.csv       /tmp/synthetic_100m/3_normal1_1
hdfs dfs -put ./data/t_100000000n_normal1_2.csv       /tmp/synthetic_100m/3_normal1_2
hdfs dfs -put ./data/t_100000000n_normal2_1.csv       /tmp/synthetic_100m/4_normal2_1
hdfs dfs -put ./data/t_100000000n_normal2_2.csv       /tmp/synthetic_100m/4_normal2_2
hdfs dfs -put ./data/t_100000000n_powerlaw1_1.csv       /tmp/synthetic_100m/5_powerlaw1_1
hdfs dfs -put ./data/t_100000000n_powerlaw1_2.csv       /tmp/synthetic_100m/5_powerlaw1_2
hdfs dfs -put ./data/t_100000000n_powerlaw2_1.csv       /tmp/synthetic_100m/6_powerlaw2_1
hdfs dfs -put ./data/t_100000000n_powerlaw2_2.csv       /tmp/synthetic_100m/6_powerlaw2_2


# hdfs dfs -mkdir -p /tmp/synthetic_100m/uniform3_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/uniform3_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/normal3_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/normal3_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw3_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw3_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw4_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw4_2
# hdfs dfs -put ./data/t_100000000n_uniform3_1.csv       /tmp/synthetic_100m/uniform3_1
# hdfs dfs -put ./data/t_100000000n_uniform3_2.csv       /tmp/synthetic_100m/uniform3_2
# hdfs dfs -put ./data/t_100000000n_normal3_1.csv       /tmp/synthetic_100m/normal3_1
# hdfs dfs -put ./data/t_100000000n_normal3_2.csv       /tmp/synthetic_100m/normal3_2
# hdfs dfs -put ./data/t_100000000n_powerlaw4_1.csv       /tmp/synthetic_100m/powerlaw4_1
# hdfs dfs -put ./data/t_100000000n_powerlaw4_2.csv       /tmp/synthetic_100m/powerlaw4_2

# hdfs dfs -mkdir -p /tmp/synthetic_100m/uniform1_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/uniform1_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/uniform2_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/uniform2_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/normal1_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/normal1_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/normal2_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/normal2_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw1_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw1_2
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw2_1
# hdfs dfs -mkdir -p /tmp/synthetic_100m/powerlaw2_2

# hdfs dfs -put ./data/t_100000000n_uniform1_1.csv       /tmp/synthetic_100m/uniform1_1
# hdfs dfs -put ./data/t_100000000n_uniform1_2.csv       /tmp/synthetic_100m/uniform1_2
# hdfs dfs -put ./data/t_100000000n_uniform2_1.csv       /tmp/synthetic_100m/uniform2_1
# hdfs dfs -put ./data/t_100000000n_uniform2_2.csv       /tmp/synthetic_100m/uniform2_2
# hdfs dfs -put ./data/t_100000000n_normal1_1.csv       /tmp/synthetic_100m/normal1_1
# hdfs dfs -put ./data/t_100000000n_normal1_2.csv       /tmp/synthetic_100m/normal1_2
# hdfs dfs -put ./data/t_100000000n_normal2_1.csv       /tmp/synthetic_100m/normal2_1
# hdfs dfs -put ./data/t_100000000n_normal2_2.csv       /tmp/synthetic_100m/normal2_2
# hdfs dfs -put ./data/t_100000000n_powerlaw1_1.csv       /tmp/synthetic_100m/powerlaw1_1
# hdfs dfs -put ./data/t_100000000n_powerlaw1_2.csv       /tmp/synthetic_100m/powerlaw1_2
# hdfs dfs -put ./data/t_100000000n_powerlaw2_1.csv       /tmp/synthetic_100m/powerlaw2_1
# hdfs dfs -put ./data/t_100000000n_powerlaw2_2.csv       /tmp/synthetic_100m/powerlaw2_2