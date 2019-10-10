#!/bin/bash
hdfs dfs -mkdir -p /tmp/instacart/aisles
hdfs dfs -mkdir -p /tmp/instacart/orders
hdfs dfs -mkdir -p /tmp/instacart/products
hdfs dfs -mkdir -p /tmp/instacart/departments
hdfs dfs -mkdir -p /tmp/instacart/order_products
hdfs dfs -put ./data/aisles.csv       /tmp/instacart/aisles
hdfs dfs -put ./data/orders.csv       /tmp/instacart/orders
hdfs dfs -put ./data/products.csv   /tmp/instacart/products
hdfs dfs -put ./data/departments.csv   /tmp/instacart/departments
hdfs dfs -put ./data/order_products__prior.csv           /tmp/instacart/order_products
