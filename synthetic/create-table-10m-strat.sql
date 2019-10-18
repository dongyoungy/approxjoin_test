CREATE DATABASE IF NOT EXISTS synthetic_10m_text;

CREATE EXTERNAL TABLE IF NOT EXISTS synthetic_10m_text.normal_powerlaw2_1 (
  col1  INT,
  col2  INT,
  col3  INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/tmp/synthetic_10m/normal_powerlaw2_1';

CREATE TABLE synthetic_10m.normal_powerlaw2_1 STORED AS parquet AS SELECT * FROM synthetic_10m_text.normal_powerlaw2_1;
COMPUTE STATS synthetic_10m.normal_powerlaw2_1;