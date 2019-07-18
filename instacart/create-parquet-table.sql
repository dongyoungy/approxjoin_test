CREATE SCHEMA IF NOT EXISTS instacart;
CREATE TABLE instacart.aisles STORED AS parquet AS SELECT * FROM instacart_text.aisles;
CREATE TABLE instacart.orders STORED AS parquet AS SELECT * FROM instacart_text.orders;
CREATE TABLE instacart.products STORED AS parquet AS SELECT * FROM instacart_text.products;
CREATE TABLE instacart.departments STORED AS parquet AS SELECT * FROM instacart_text.departments;
CREATE TABLE instacart.order_products STORED AS parquet AS SELECT * FROM instacart_text.order_products;

COMPUTE STATS instacart.aisles;
COMPUTE STATS instacart.orders;
COMPUTE STATS instacart.products;
COMPUTE STATS instacart.departments;
COMPUTE STATS instacart.order_products;
