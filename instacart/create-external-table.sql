CREATE DATABASE IF NOT EXISTS instacart_text;
CREATE EXTERNAL TABLE IF NOT EXISTS instacart_text.aisles (
  aisle_id  INT,
  aisle     VARCHAR(100)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/tmp/instacart/aisles';
CREATE EXTERNAL TABLE IF NOT EXISTS instacart_text.orders ( 
  order_id           INT,
  user_id            INT,
  eval_set           VARCHAR(10),
  order_number       INT,
  order_dow          INT,
  order_hour_of_day  INT,
  days_since_prior   DECIMAL(5,1)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/tmp/instacart/orders';
CREATE EXTERNAL TABLE IF NOT EXISTS instacart_text.products ( 
  product_id     INT,
  product_name   VARCHAR(500),
  aisle_id       INT,
  department_id  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/tmp/instacart/products';
CREATE EXTERNAL TABLE IF NOT EXISTS instacart_text.departments ( 
  department_id  INT,
  department     VARCHAR(100)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/tmp/instacart/departments';
CREATE EXTERNAL TABLE IF NOT EXISTS instacart_text.order_products ( 
order_id          INT,
product_id        INT,
add_to_car_order  INT,
reordered         INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/tmp/instacart/order_products';
